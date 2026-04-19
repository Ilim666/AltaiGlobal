import re
import os
import secrets
from io import BytesIO
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from functools import wraps

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_sqlalchemy import SQLAlchemy
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import case, func, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY") or secrets.token_hex(32)
db = SQLAlchemy(app)


def fmt_phone(phone):
    """Format a 10-digit phone string as XXXX-XXX-XXX."""
    digits = re.sub(r"\D", "", str(phone or ""))
    if len(digits) == 10:
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return phone or ""


app.jinja_env.filters["fmt_phone"] = fmt_phone


# ── Models ────────────────────────────────────────────────────────────────────

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="operator")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fio = db.Column(db.String(100), unique=True, nullable=False)
    phone = db.Column(db.String(10), nullable=False)
    inn = db.Column(db.String(14), nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    cars = db.relationship("Car", backref="client", lazy=True, cascade="all, delete-orphan")


class Car(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    number = db.Column(db.String(20), unique=True, nullable=False)
    brand = db.Column(db.String(100), nullable=False)
    color = db.Column(db.String(50), nullable=False)
    note = db.Column(db.Text, nullable=True)
    stock = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    sales = db.relationship("Sale", backref="car", lazy=True, cascade="all, delete-orphan")
    receipts = db.relationship("Receipt", backref="car", lazy=True, cascade="all, delete-orphan")


PAYMENT_METHODS = ["наличка", "безнал", "доллар", "долг"]


class Sale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    car_id = db.Column(db.Integer, db.ForeignKey("car.id"), nullable=False)
    liters = db.Column(db.Float, nullable=False)
    price_per_liter = db.Column(db.Float, nullable=False)
    total = db.Column(db.Float, nullable=False)
    payment_method = db.Column(db.String(20), nullable=False)
    payment_amount = db.Column(db.Float, nullable=True)
    note = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    payments = db.relationship("Payment", backref="sale", lazy=True, cascade="all, delete-orphan")


class Receipt(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    car_id = db.Column(db.Integer, db.ForeignKey("car.id"), nullable=False)
    liters = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    notes = db.Column(db.Text, nullable=True)


class DailyStock(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    stock_date = db.Column(db.Date, nullable=False, unique=True)
    current_stock = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class StockHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    stock_date = db.Column(db.Date, nullable=False)
    added_liters = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


PAYMENT_TYPES = ["продажа", "долг"]
DEBT_PAYMENT_TYPE = "долг"

REPORT_TYPE_LABELS = {
    "sales": "Журнал продаж",
    "payments": "Журнал оплат",
    "cash": "Касса",
    "turnover": "Оборот",
}


class Payment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    sale_id = db.Column(db.Integer, db.ForeignKey("sale.id"), nullable=True)
    amount = db.Column(db.Float, nullable=False)
    payment_type = db.Column(
        db.String(20),
        db.CheckConstraint("payment_type IN ('продажа', 'долг')"),
        nullable=False,
    )
    payment_method = db.Column(db.String(20), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    client = db.relationship("Client", backref=db.backref("payments", lazy=True))


# ── Helpers ───────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session or session.get("role") != "admin":
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated_function


def verify_user_password(user, password):
    if not user.password:
        return False
    return check_password_hash(user.password, password)


def validate_phone(phone):
    digits = re.sub(r"\D", "", phone)
    return digits if len(digits) == 10 else None


def validate_inn(inn):
    digits = re.sub(r"\D", "", inn)
    return digits if len(digits) == 14 else None


def _parse_iso_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _coerce_day(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _parse_month_value(month_value):
    if not month_value:
        return None
    try:
        return datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError:
        return None


def _month_bounds(month_value=None):
    month_start = _parse_month_value(month_value)
    if not month_start:
        today = datetime.now(timezone.utc).date()
        month_start = today.replace(day=1)
    next_month_start = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_end = next_month_start - timedelta(days=1)
    return month_start, month_end, month_start.strftime("%Y-%m")


def _month_label(month_start):
    months_ru = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
    ]
    return f"{months_ru[month_start.month - 1]} {month_start.year}"


def _month_label_filename(month_start):
    return _month_label(month_start).replace(" ", "")


def _format_number(value):
    return round(float(value or 0), 2)


def _month_datetime_bounds(start_date, end_date):
    return datetime.combine(start_date, time.min), datetime.combine(end_date + timedelta(days=1), time.min)


def _remaining_goods_by_day(days):
    if not days:
        return {}

    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())
    table_name = "car" if "car" in table_names else "cars" if "cars" in table_names else None
    if not table_name:
        return {}

    candidate_remaining_columns = [
        "remaining_goods",
        "stock_remaining",
        "fuel_remaining",
        "liters_remaining",
        "remainder",
    ]
    columns = {column["name"] for column in inspector.get_columns(table_name)}
    remaining_column = next((name for name in candidate_remaining_columns if name in columns), None)
    if not remaining_column:
        return {}

    date_column = next((name for name in ["updated_at", "created_at"] if name in columns), None)

    if not date_column:
        total_remaining = db.session.execute(
            text(f"SELECT COALESCE(SUM({remaining_column}), 0) FROM {table_name}")
        ).scalar_one()
        return {day: float(total_remaining or 0) for day in days}

    min_day = min(days).isoformat()
    max_day = max(days).isoformat()
    rows = db.session.execute(
        text(
            f"""
            SELECT DATE({date_column}) AS d, COALESCE(SUM({remaining_column}), 0) AS rem
            FROM {table_name}
            WHERE DATE({date_column}) BETWEEN :min_day AND :max_day
            GROUP BY DATE({date_column})
            """
        ),
        {"min_day": min_day, "max_day": max_day},
    ).all()
    return {datetime.strptime(row.d, "%Y-%m-%d").date(): float(row.rem or 0) for row in rows if row.d}


def _ensure_car_stock_column():
    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())
    table_name = "car" if "car" in table_names else "cars" if "cars" in table_names else None
    if not table_name:
        return

    columns = {column["name"] for column in inspector.get_columns(table_name)}
    if "stock" in columns:
        return

    if table_name == "car":
        db.session.execute(text("ALTER TABLE car ADD COLUMN stock FLOAT DEFAULT 0"))
    else:
        db.session.execute(text("ALTER TABLE cars ADD COLUMN stock FLOAT DEFAULT 0"))
    db.session.commit()


def _ensure_daily_stock_tables():
    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())
    if "daily_stock" not in table_names:
        DailyStock.__table__.create(bind=db.engine, checkfirst=True)
    if "stock_history" not in table_names:
        StockHistory.__table__.create(bind=db.engine, checkfirst=True)


def _get_or_create_daily_stock(stock_date):
    daily_stock = DailyStock.query.filter_by(stock_date=stock_date).first()
    if daily_stock:
        return daily_stock

    previous_day_stock = (
        DailyStock.query
        .filter(DailyStock.stock_date < stock_date)
        .order_by(DailyStock.stock_date.desc())
        .first()
    )
    base_stock = float(previous_day_stock.current_stock or 0) if previous_day_stock else 0.0
    daily_stock = DailyStock(stock_date=stock_date, current_stock=base_stock)
    db.session.add(daily_stock)
    db.session.flush()
    return daily_stock


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("index"))

    if request.method == "POST":
        csrf_token = request.form.get("csrf_token", "")
        if not csrf_token or not secrets.compare_digest(csrf_token, session.get("csrf_token", "")):
            return render_template("login.html", error="Сессия истекла. Повторите вход.")

        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        user = User.query.filter_by(username=username).first()
        if user and verify_user_password(user, password):
            session["user_id"] = user.id
            session["username"] = user.username
            session["role"] = user.role
            session.pop("csrf_token", None)
            return redirect(url_for("index"))

        error = "Неверное имя пользователя или пароль"
        return render_template("login.html", error=error)

    session["csrf_token"] = secrets.token_urlsafe(32)
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    if session.get("role") == "admin":
        return redirect(url_for("clients"))
    return redirect(url_for("sales"))


@app.route("/clients")
@admin_required
def clients():
    all_clients = Client.query.order_by(Client.id.desc()).all()
    return render_template("clients.html", clients=all_clients)


@app.route("/admin/users")
@admin_required
def admin_users():
    users = User.query.order_by(User.id.asc()).all()
    return render_template("admin/users.html", users=users)


@app.route("/admin/users/add", methods=["GET", "POST"])
@admin_required
def admin_add_user():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        role = request.form.get("role", "operator")
        if role not in {"operator", "admin"}:
            role = "operator"

        if not username or not password:
            flash("Заполните все поля!", "danger")
            return redirect(url_for("admin_add_user"))

        if User.query.filter_by(username=username).first():
            flash("Пользователь уже существует!", "danger")
            return redirect(url_for("admin_add_user"))

        user = User(username=username, password=generate_password_hash(password), role=role)
        db.session.add(user)
        db.session.commit()
        flash(f"Пользователь {username} создан!", "success")
        return redirect(url_for("admin_users"))

    return render_template("admin/add_user.html")


@app.route("/admin/users/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def admin_edit_user(user_id):
    user = User.query.get_or_404(user_id)

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        role = request.form.get("role", "operator")
        if role not in {"operator", "admin"}:
            role = "operator"

        if not username:
            flash("Заполните имя пользователя!", "danger")
            return redirect(url_for("admin_edit_user", user_id=user_id))

        if username != user.username and User.query.filter_by(username=username).first():
            flash("Имя пользователя уже используется!", "danger")
            return redirect(url_for("admin_edit_user", user_id=user_id))

        user.username = username
        if password:
            user.password = generate_password_hash(password)
        user.role = role
        db.session.commit()
        flash("Пользователь обновлен!", "success")
        return redirect(url_for("admin_users"))

    return render_template("admin/edit_user.html", user=user)


@app.route("/admin/users/<int:user_id>/delete", methods=["POST"])
@admin_required
def admin_delete_user(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == session.get("user_id"):
        flash("Нельзя удалить свой аккаунт!", "danger")
        return redirect(url_for("admin_users"))

    username = user.username
    db.session.delete(user)
    db.session.commit()
    flash(f"Пользователь {username} удален!", "success")
    return redirect(url_for("admin_users"))


@app.route("/add-client", methods=["GET", "POST"])
@admin_required
def add_client():
    errors = {}
    form = {}
    if request.method == "POST":
        form["fio"] = request.form.get("fio", "").strip()
        form["phone"] = request.form.get("phone", "").strip()
        form["inn"] = request.form.get("inn", "").strip()
        form["car_number"] = request.form.get("car_number", "").strip()
        form["car_brand"] = request.form.get("car_brand", "").strip()
        form["car_color"] = request.form.get("car_color", "").strip()
        form["car_note"] = request.form.get("car_note", "").strip()

        if not form["fio"]:
            errors["fio"] = "ФИО обязательно."
        elif Client.query.filter_by(fio=form["fio"]).first():
            errors["fio"] = "Клиент с таким ФИО уже существует."

        phone_digits = validate_phone(form["phone"])
        if phone_digits is None:
            errors["phone"] = "Телефон должен содержать ровно 10 цифр."
        elif Client.query.filter_by(phone=phone_digits).first():
            errors["phone"] = "Телефон уже используется."

        inn_digits = validate_inn(form["inn"])
        if inn_digits is None:
            errors["inn"] = "ИНН должен содержать ровно 14 цифр."
        elif Client.query.filter_by(inn=inn_digits).first():
            errors["inn"] = "ИНН уже используется."

        if not form["car_number"]:
            errors["car_number"] = "Номер машины обязателен."
        elif Car.query.filter_by(number=form["car_number"]).first():
            errors["car_number"] = "Машина с таким номером уже существует."

        if not form["car_brand"]:
            errors["car_brand"] = "Марка машины обязательна."

        if not form["car_color"]:
            errors["car_color"] = "Цвет машины обязателен."

        if not errors:
            client = Client(fio=form["fio"], phone=phone_digits, inn=inn_digits)
            db.session.add(client)
            db.session.flush()
            car = Car(
                client_id=client.id,
                number=form["car_number"],
                brand=form["car_brand"],
                color=form["car_color"],
                note=form["car_note"] or None,
            )
            db.session.add(car)
            db.session.commit()
            return redirect(url_for("client_detail", id=client.id))

    return render_template("add_client.html", errors=errors, form=form)


@app.route("/client/<int:id>")
@admin_required
def client_detail(id):
    client = Client.query.get_or_404(id)
    cars = Car.query.filter_by(client_id=id).order_by(Car.id.desc()).all()
    return render_template("client_detail.html", client=client, cars=cars)


@app.route("/edit-client/<int:id>", methods=["GET", "POST"])
@admin_required
def edit_client(id):
    client = Client.query.get_or_404(id)
    errors = {}
    form = {
        "fio": client.fio,
        "phone": client.phone,
        "inn": client.inn,
    }
    if request.method == "POST":
        form["fio"] = request.form.get("fio", "").strip()
        form["phone"] = request.form.get("phone", "").strip()
        form["inn"] = request.form.get("inn", "").strip()

        if not form["fio"]:
            errors["fio"] = "ФИО обязательно."
        else:
            existing = Client.query.filter_by(fio=form["fio"]).first()
            if existing and existing.id != id:
                errors["fio"] = "Клиент с таким ФИО уже существует."

        phone_digits = validate_phone(form["phone"])
        if phone_digits is None:
            errors["phone"] = "Телефон должен содержать ровно 10 цифр."
        else:
            existing_phone = Client.query.filter_by(phone=phone_digits).first()
            if existing_phone and existing_phone.id != id:
                errors["phone"] = "Телефон уже используется."

        inn_digits = validate_inn(form["inn"])
        if inn_digits is None:
            errors["inn"] = "ИНН должен содержать ровно 14 цифр."
        else:
            existing_inn = Client.query.filter_by(inn=inn_digits).first()
            if existing_inn and existing_inn.id != id:
                errors["inn"] = "ИНН уже используется."

        if not errors:
            client.fio = form["fio"]
            client.phone = phone_digits
            client.inn = inn_digits
            db.session.commit()
            return redirect(url_for("client_detail", id=id))

    return render_template("edit_client.html", client=client, errors=errors, form=form)


@app.route("/delete-client/<int:id>", methods=["POST"])
@admin_required
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


@app.route("/add-car", methods=["GET", "POST"])
@admin_required
def add_car():
    all_clients = Client.query.order_by(Client.fio).all()
    errors = {}
    form = {}
    if request.method == "POST":
        form["client_id"] = request.form.get("client_id", "").strip()
        form["number"] = request.form.get("number", "").strip()
        form["brand"] = request.form.get("brand", "").strip()
        form["color"] = request.form.get("color", "").strip()
        form["note"] = request.form.get("note", "").strip()

        if not form["client_id"]:
            errors["client_id"] = "Выберите клиента."

        if not form["number"]:
            errors["number"] = "Номер машины обязателен."
        elif Car.query.filter_by(number=form["number"]).first():
            errors["number"] = "Машина с таким номером уже существует."

        if not form["brand"]:
            errors["brand"] = "Марка машины обязательна."

        if not form["color"]:
            errors["color"] = "Цвет машины обязателен."

        if not errors:
            car = Car(
                client_id=int(form["client_id"]),
                number=form["number"],
                brand=form["brand"],
                color=form["color"],
                note=form["note"] or None,
            )
            db.session.add(car)
            db.session.commit()
            return redirect(url_for("client_detail", id=car.client_id))

    return render_template("add_car.html", clients=all_clients, errors=errors, form=form)


@app.route("/edit-car/<int:id>", methods=["GET", "POST"])
@admin_required
def edit_car(id):
    car = Car.query.get_or_404(id)
    errors = {}
    form = {
        "number": car.number,
        "brand": car.brand,
        "color": car.color,
        "note": car.note or "",
    }
    if request.method == "POST":
        form["number"] = request.form.get("number", "").strip()
        form["brand"] = request.form.get("brand", "").strip()
        form["color"] = request.form.get("color", "").strip()
        form["note"] = request.form.get("note", "").strip()

        if not form["number"]:
            errors["number"] = "Номер машины обязателен."
        else:
            existing = Car.query.filter_by(number=form["number"]).first()
            if existing and existing.id != id:
                errors["number"] = "Машина с таким номером уже существует."

        if not form["brand"]:
            errors["brand"] = "Марка машины обязательна."

        if not form["color"]:
            errors["color"] = "Цвет машины обязателен."

        if not errors:
            car.number = form["number"]
            car.brand = form["brand"]
            car.color = form["color"]
            car.note = form["note"] or None
            db.session.commit()
            return redirect(url_for("client_detail", id=car.client_id))

    return render_template("edit_car.html", car=car, errors=errors, form=form)


@app.route("/cars")
@admin_required
def cars():
    q = request.args.get("q", "").strip()
    query = Car.query.options(joinedload(Car.client)).join(Car.client)
    if q:
        query = query.filter(Client.fio.ilike(f"%{q}%"))
    all_cars = query.order_by(Car.id.desc()).all()
    return render_template("cars.html", cars=all_cars, q=q)


@app.route("/delete-car/<int:id>", methods=["POST"])
@admin_required
def delete_car(id):
    car = Car.query.get_or_404(id)
    client_id = car.client_id
    next_page = request.form.get("next", "")
    db.session.delete(car)
    db.session.commit()
    if next_page == "cars":
        return redirect(url_for("cars"))
    return redirect(url_for("client_detail", id=client_id))


# ── Validation API ─────────────────────────────────────────────────────────────

@app.route("/api/check-fio", methods=["POST"])
@admin_required
def check_fio():
    payload = request.get_json(silent=True) or {}
    fio = payload.get("fio", "").strip()
    if not fio:
        return jsonify({"exists": False})

    existing = Client.query.filter_by(fio=fio).first()
    return jsonify({"exists": bool(existing)})


@app.route("/api/check-phone", methods=["POST"])
@admin_required
def check_phone():
    payload = request.get_json(silent=True) or {}
    phone = payload.get("phone", "").strip()
    client_id = payload.get("client_id")
    digits = re.sub(r"\D", "", phone)
    is_valid = len(digits) == 10

    exists = False
    if is_valid:
        query = Client.query.filter_by(phone=digits)
        if client_id is not None and client_id != "":
            try:
                query = query.filter(Client.id != int(client_id))
            except (TypeError, ValueError):
                pass
        exists = bool(query.first())

    return jsonify({
        "valid": is_valid,
        "exists": exists,
        "formatted": fmt_phone(digits) if is_valid else "",
    })


@app.route("/api/check-inn", methods=["POST"])
@admin_required
def check_inn():
    payload = request.get_json(silent=True) or {}
    inn = payload.get("inn", "").strip()
    client_id = payload.get("client_id")
    digits = re.sub(r"\D", "", inn)
    is_valid = len(digits) == 14

    exists = False
    if is_valid:
        query = Client.query.filter_by(inn=digits)
        if client_id is not None and client_id != "":
            try:
                query = query.filter(Client.id != int(client_id))
            except (TypeError, ValueError):
                pass
        exists = bool(query.first())

    return jsonify({"valid": is_valid, "exists": exists})


@app.route("/api/check-car-number", methods=["POST"])
@admin_required
def check_car_number():
    payload = request.get_json(silent=True) or {}
    car_number = payload.get("car_number", "").strip()
    car_id = payload.get("car_id")
    if not car_number:
        return jsonify({"exists": False})

    query = Car.query.filter_by(number=car_number)
    if car_id is not None and car_id != "":
        try:
            query = query.filter(Car.id != int(car_id))
        except (TypeError, ValueError):
            pass
    existing = query.first()
    return jsonify({"exists": bool(existing)})


@app.route("/api/increase-stock", methods=["POST"])
@admin_required
def increase_stock():
    _ensure_car_stock_column()

    payload = request.get_json(silent=True) or {}
    car_id_raw = payload.get("car_id")
    liters_raw = payload.get("liters")

    try:
        car_id = int(car_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Выберите корректную машину."}), 400

    try:
        liters = float(liters_raw)
        if liters <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Количество литров должно быть больше 0."}), 400

    car = Car.query.get(car_id)
    if not car:
        return jsonify({"ok": False, "error": "Машина не найдена."}), 404

    car.stock = round(float(car.stock or 0) + liters, 2)
    db.session.commit()

    app.logger.info(
        "Stock increased: car_id=%s, liters=%s, user_id=%s",
        car_id,
        liters,
        session.get("user_id"),
    )

    return jsonify({"ok": True, "car_id": car.id, "stock": car.stock})


@app.route("/api/set-daily-stock", methods=["POST"])
@admin_required
def set_daily_stock():
    _ensure_daily_stock_tables()

    payload = request.get_json(silent=True) or {}
    liters_raw = payload.get("liters")

    try:
        liters = float(liters_raw)
        if liters <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Количество литров должно быть больше 0."}), 400

    today = datetime.now(timezone.utc).date()
    daily_stock = _get_or_create_daily_stock(today)
    daily_stock.current_stock = round(float(daily_stock.current_stock) + liters, 2)
    db.session.add(StockHistory(stock_date=today, added_liters=liters))
    db.session.commit()

    app.logger.info(
        "Daily stock increased: stock_date=%s, liters=%s, user_id=%s",
        today.isoformat(),
        liters,
        session.get("user_id"),
    )

    return jsonify({
        "ok": True,
        "stock_date": today.isoformat(),
        "current_stock": daily_stock.current_stock,
        "added_liters": liters,
    })


# ── Sales ─────────────────────────────────────────────────────────────────────

@app.route("/api/car-search")
@login_required
def api_car_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    cars = (
        Car.query.options(joinedload(Car.client))
        .filter(Car.number.ilike(f"%{q}%"))
        .limit(10)
        .all()
    )
    return jsonify([
        {"id": c.id, "number": c.number, "fio": c.client.fio}
        for c in cars
    ])


@app.route("/receipts", methods=["GET", "POST"])
@login_required
def receipts():
    start_date = _parse_iso_date(request.args.get("start_date"))
    end_date = _parse_iso_date(request.args.get("end_date"))
    errors = {}
    form = {}

    if request.method == "POST":
        form["car_id"] = request.form.get("car_id", "").strip()
        form["liters"] = request.form.get("liters", "").strip()
        form["amount"] = request.form.get("amount", "").strip()
        form["notes"] = request.form.get("notes", "").strip()

        car = None
        if not form["car_id"]:
            errors["car_id"] = "Выберите машину."
        else:
            try:
                car = Car.query.get(int(form["car_id"]))
                if not car:
                    errors["car_id"] = "Машина не найдена."
            except (TypeError, ValueError):
                errors["car_id"] = "Выберите корректную машину."

        try:
            liters = float(form["liters"])
            if liters <= 0:
                raise ValueError
        except (ValueError, TypeError):
            errors["liters"] = "Введите корректное количество литров."
            liters = None

        try:
            amount = float(form["amount"])
            if amount <= 0:
                raise ValueError
        except (ValueError, TypeError):
            errors["amount"] = "Введите корректную сумму."
            amount = None

        if not errors:
            db.session.add(
                Receipt(
                    car_id=car.id,
                    liters=liters,
                    amount=amount,
                    notes=form["notes"] or None,
                )
            )
            db.session.commit()
            return redirect(url_for("receipts"))

    cars = Car.query.options(joinedload(Car.client)).order_by(Car.number.asc()).all()
    query = Receipt.query.options(joinedload(Receipt.car).joinedload(Car.client))
    if start_date:
        query = query.filter(Receipt.created_at >= datetime.combine(start_date, time.min))
    if end_date:
        query = query.filter(Receipt.created_at < datetime.combine(end_date + timedelta(days=1), time.min))
    all_receipts = query.order_by(Receipt.created_at.desc()).all()

    return render_template(
        "receipts.html",
        receipts=all_receipts,
        cars=cars,
        errors=errors,
        form=form,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
    )


@app.route("/receipts/<int:receipt_id>", methods=["POST", "DELETE"])
@admin_required
def delete_receipt(receipt_id):
    receipt = Receipt.query.get_or_404(receipt_id)
    db.session.delete(receipt)
    db.session.commit()
    return redirect(url_for("receipts"))


@app.route("/sales", methods=["GET", "POST"])
@login_required
def sales():
    errors = {}
    form = {}
    client_info = None
    if request.method == "POST":
        form["car_number"] = request.form.get("car_number", "").strip()
        form["liters"] = request.form.get("liters", "").strip()
        form["price_per_liter"] = request.form.get("price_per_liter", "").strip()
        form["payment_method"] = request.form.get("payment_method", "").strip()
        form["payment_amount"] = request.form.get("payment_amount", "").strip()
        form["note"] = request.form.get("note", "").strip()

        car = Car.query.filter_by(number=form["car_number"]).first() if form["car_number"] else None
        if not form["car_number"]:
            errors["car_number"] = "Введите номер машины."
        elif not car:
            errors["car_number"] = "Машина с таким номером не найдена."

        try:
            liters = float(form["liters"])
            if liters <= 0:
                raise ValueError
        except (ValueError, TypeError):
            errors["liters"] = "Введите корректное количество литров."
            liters = None

        try:
            price = float(form["price_per_liter"])
            if price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            errors["price_per_liter"] = "Введите корректную цену за литр."
            price = None

        if form["payment_method"] not in PAYMENT_METHODS:
            errors["payment_method"] = "Выберите способ оплаты."

        payment_amount = None
        if form["payment_method"] != "долг":
            if not form["payment_amount"]:
                errors["payment_amount"] = "Введите сумму оплаты."
            else:
                try:
                    payment_amount = float(form["payment_amount"])
                    if payment_amount < 0:
                        raise ValueError
                except (ValueError, TypeError):
                    errors["payment_amount"] = "Введите корректную сумму оплаты."

        if not errors:
            total = round(liters * price, 2)
            sale = Sale(
                car_id=car.id,
                liters=liters,
                price_per_liter=price,
                total=total,
                payment_method=form["payment_method"],
                payment_amount=payment_amount,
                note=form["note"] or None,
            )
            db.session.add(sale)
            db.session.flush()
            if form["payment_method"] != "долг" and payment_amount:
                payment = Payment(
                    client_id=car.client_id,
                    sale_id=sale.id,
                    amount=payment_amount,
                    payment_type="продажа",
                    payment_method=form["payment_method"],
                )
                db.session.add(payment)
            _ensure_daily_stock_tables()
            sale_date = datetime.now(timezone.utc).date()
            daily_stock = _get_or_create_daily_stock(sale_date)
            daily_stock.current_stock = round(float(daily_stock.current_stock) - liters, 2)
            db.session.commit()
            return redirect(url_for("sales_journal"))

        if car:
            client_info = {"fio": car.client.fio}

    return render_template(
        "sales.html",
        errors=errors,
        form=form,
        client_info=client_info,
        payment_methods=PAYMENT_METHODS,
    )


def _build_sales_report_payload(start_date, end_date):
    start_dt, end_dt = _month_datetime_bounds(start_date, end_date)
    sales = (
        Sale.query
        .options(joinedload(Sale.car).joinedload(Car.client))
        .join(Sale.car)
        .join(Car.client)
        .filter(Sale.created_at >= start_dt, Sale.created_at < end_dt)
        .order_by(Sale.created_at.desc())
        .all()
    )
    rows = []
    for sale in sales:
        payment_amount = _format_number(sale.payment_amount)
        total = _format_number(sale.total)
        rows.append(
            {
                "date": sale.created_at.strftime("%d.%m.%Y %H:%M"),
                "client": sale.car.client.fio,
                "car_number": sale.car.number,
                "liters": _format_number(sale.liters),
                "price_per_liter": _format_number(sale.price_per_liter),
                "total": total,
                "payment_amount": payment_amount if sale.payment_amount is not None else "—",
                "payment_method": sale.payment_method or "—",
                "remaining": _format_number(total - payment_amount),
                "note": sale.note or "—",
            }
        )
    return {
        "columns": [
            {"key": "date", "label": "Дата"},
            {"key": "client", "label": "Клиент"},
            {"key": "car_number", "label": "Номер машины"},
            {"key": "liters", "label": "Литры"},
            {"key": "price_per_liter", "label": "Цена / л"},
            {"key": "total", "label": "Сумма"},
            {"key": "payment_amount", "label": "Сумма оплаты"},
            {"key": "payment_method", "label": "Способ оплаты"},
            {"key": "remaining", "label": "Остаток суммы"},
            {"key": "note", "label": "Примечание"},
        ],
        "rows": rows,
    }


def _build_payments_report_payload(start_date, end_date):
    start_dt, end_dt = _month_datetime_bounds(start_date, end_date)
    payments_data = (
        Payment.query
        .options(joinedload(Payment.sale).joinedload(Sale.car), joinedload(Payment.client))
        .join(Payment.client)
        .filter(Payment.created_at >= start_dt, Payment.created_at < end_dt)
        .order_by(Payment.created_at.desc())
        .all()
    )
    rows = []
    for payment in payments_data:
        sale = payment.sale
        remaining = _format_number((sale.total - (sale.payment_amount or 0)) if sale else 0)
        rows.append(
            {
                "sale_date": sale.created_at.strftime("%d.%m.%Y %H:%M") if sale else "—",
                "client": payment.client.fio,
                "car_number": sale.car.number if sale and sale.car else "—",
                "liters": _format_number(sale.liters) if sale else "—",
                "price_per_liter": _format_number(sale.price_per_liter) if sale else "—",
                "total": _format_number(sale.total) if sale else "—",
                "payment_amount": _format_number(payment.amount),
                "payment_method": payment.payment_method or (sale.payment_method if sale else "—"),
                "remaining": remaining,
                "payment_date": payment.created_at.strftime("%d.%m.%Y %H:%M"),
            }
        )
    return {
        "columns": [
            {"key": "sale_date", "label": "Дата продажи"},
            {"key": "client", "label": "Клиент"},
            {"key": "car_number", "label": "Номер машины"},
            {"key": "liters", "label": "Литры"},
            {"key": "price_per_liter", "label": "Цена / л"},
            {"key": "total", "label": "Сумма"},
            {"key": "payment_amount", "label": "Сумма оплаты"},
            {"key": "payment_method", "label": "Способ оплаты"},
            {"key": "remaining", "label": "Остаток суммы"},
            {"key": "payment_date", "label": "Дата и время оплаты"},
        ],
        "rows": rows,
    }


def _build_cash_rows(start_date, end_date):
    start_dt, end_dt = _month_datetime_bounds(start_date, end_date)
    all_payments = (
        Payment.query
        .filter(Payment.created_at >= start_dt, Payment.created_at < end_dt)
        .order_by(Payment.created_at.desc())
        .all()
    )
    daily = {}
    for payment in all_payments:
        date_key = payment.created_at.strftime("%d.%m.%Y")
        if date_key not in daily:
            daily[date_key] = {
                "date": date_key,
                "sale_наличка": 0.0,
                "sale_безнал": 0.0,
                "sale_доллар": 0.0,
                "debt_наличка": 0.0,
                "debt_безнал": 0.0,
                "debt_доллар": 0.0,
            }
        method = payment.payment_method or ""
        if payment.payment_type == "продажа" and method in ("наличка", "безнал", "доллар"):
            daily[date_key][f"sale_{method}"] += payment.amount
        elif payment.payment_type == "долг" and method in ("наличка", "безнал", "доллар"):
            daily[date_key][f"debt_{method}"] += payment.amount

    rows = []
    for data in daily.values():
        row = dict(data)
        row["total_наличка"] = _format_number(row["sale_наличка"] + row["debt_наличка"])
        row["total_безнал"] = _format_number(row["sale_безнал"] + row["debt_безнал"])
        row["total_доллар"] = _format_number(row["sale_доллар"] + row["debt_доллар"])
        row["sale_наличка"] = _format_number(row["sale_наличка"])
        row["sale_безнал"] = _format_number(row["sale_безнал"])
        row["sale_доллар"] = _format_number(row["sale_доллар"])
        row["debt_наличка"] = _format_number(row["debt_наличка"])
        row["debt_безнал"] = _format_number(row["debt_безнал"])
        row["debt_доллар"] = _format_number(row["debt_доллар"])
        rows.append(row)
    rows.sort(key=lambda row: datetime.strptime(row["date"], "%d.%m.%Y"), reverse=True)
    return rows


def _build_cash_report_payload(start_date, end_date):
    return {
        "columns": [
            {"key": "date", "label": "Дата"},
            {"key": "sale_наличка", "label": "Наличка сегодня"},
            {"key": "sale_безнал", "label": "Безнал сегодня"},
            {"key": "sale_доллар", "label": "Доллар сегодня"},
            {"key": "debt_наличка", "label": "Наличка от долгов"},
            {"key": "debt_безнал", "label": "Безнал от долгов"},
            {"key": "debt_доллар", "label": "Доллар от долгов"},
            {"key": "total_наличка", "label": "Наличка всего"},
            {"key": "total_безнал", "label": "Безнал всего"},
            {"key": "total_доллар", "label": "Доллар всего"},
        ],
        "rows": _build_cash_rows(start_date, end_date),
    }


def _build_turnover_rows(start_date, end_date):
    _ensure_daily_stock_tables()
    sale_day = func.date(Sale.created_at)
    payment_method = func.lower(func.coalesce(Sale.payment_method, ""))
    min_dt, max_dt = _month_datetime_bounds(start_date, end_date)

    sales_query = db.session.query(
        sale_day.label("sale_date"),
        func.coalesce(func.sum(Sale.liters), 0).label("liters"),
        func.coalesce(func.sum(Sale.total), 0).label("amount"),
        func.coalesce(func.sum(case((payment_method == DEBT_PAYMENT_TYPE, 0), else_=func.coalesce(Sale.payment_amount, 0))), 0).label("payments"),
        func.coalesce(func.sum(case((payment_method == DEBT_PAYMENT_TYPE, Sale.total - func.coalesce(Sale.payment_amount, 0)), else_=0)), 0).label("debts"),
    ).filter(Sale.created_at >= min_dt, Sale.created_at < max_dt)

    rows_data = []
    totals = {"liters": 0.0, "amount": 0.0, "payments": 0.0, "debts": 0.0, "average_price": 0.0}
    error_message = None

    try:
        grouped_sales = sales_query.group_by(sale_day).order_by(sale_day.desc()).all()
        sales_by_day = {}
        for row in grouped_sales:
            row_date = _coerce_day(row.sale_date)
            if not row_date:
                continue
            liters = float(row.liters or 0)
            amount = float(row.amount or 0)
            payments = float(row.payments or 0)
            debts = float(row.debts or 0)
            average_price = amount / liters if liters else 0.0
            totals["liters"] += liters
            totals["amount"] += amount
            totals["payments"] += payments
            totals["debts"] += debts
            sales_by_day[row_date] = {
                "liters": liters,
                "amount": amount,
                "payments": payments,
                "debts": debts,
                "average_price": average_price,
            }

        additions_by_day = {
            _coerce_day(row.stock_date): float(row.added_liters or 0)
            for row in db.session.query(
                StockHistory.stock_date.label("stock_date"),
                func.coalesce(func.sum(StockHistory.added_liters), 0).label("added_liters"),
            ).filter(StockHistory.stock_date >= start_date, StockHistory.stock_date <= end_date).group_by(StockHistory.stock_date).all()
            if _coerce_day(row.stock_date)
        }
        stock_by_day = {
            _coerce_day(row.stock_date): float(row.current_stock or 0)
            for row in db.session.query(
                DailyStock.stock_date.label("stock_date"),
                DailyStock.current_stock.label("current_stock"),
            ).filter(DailyStock.stock_date >= start_date, DailyStock.stock_date <= end_date).all()
            if _coerce_day(row.stock_date)
        }

        all_days = set(sales_by_day) | set(additions_by_day) | set(stock_by_day)
        for row_date in sorted(all_days, reverse=True):
            daily_sales = sales_by_day.get(
                row_date,
                {"liters": 0.0, "amount": 0.0, "payments": 0.0, "debts": 0.0, "average_price": 0.0},
            )
            rows_data.append(
                {
                    "date": row_date,
                    "date_label": row_date.strftime("%d.%m.%Y"),
                    "liters": daily_sales["liters"],
                    "amount": daily_sales["amount"],
                    "payments": daily_sales["payments"],
                    "debts": daily_sales["debts"],
                    "average_price": daily_sales["average_price"],
                    "remaining_goods": stock_by_day.get(row_date, 0.0),
                    "added_liters": additions_by_day.get(row_date, 0.0),
                }
            )
        totals["average_price"] = totals["amount"] / totals["liters"] if totals["liters"] else 0.0
    except SQLAlchemyError:
        app.logger.exception("Failed to build turnover report for date range %s - %s", start_date.isoformat(), end_date.isoformat())
        rows_data = []
        error_message = "Не удалось загрузить данные оборота. Проверьте подключение к базе данных."
    return rows_data, totals, error_message


def _build_turnover_report_payload(start_date, end_date):
    rows_data, totals, error_message = _build_turnover_rows(start_date, end_date)
    return {
        "columns": [
            {"key": "date_label", "label": "Дата"},
            {"key": "liters", "label": "Литр"},
            {"key": "amount", "label": "Сумма"},
            {"key": "payments", "label": "Оплаты"},
            {"key": "debts", "label": "Долги"},
            {"key": "average_price", "label": "Средняя цена"},
            {"key": "remaining_goods", "label": "Остаток товара"},
            {"key": "added_liters", "label": "Поступление"},
        ],
        "rows": [
            {
                "date_label": row["date_label"],
                "liters": _format_number(row["liters"]),
                "amount": _format_number(row["amount"]),
                "payments": _format_number(row["payments"]),
                "debts": _format_number(row["debts"]),
                "average_price": _format_number(row["average_price"]),
                "remaining_goods": _format_number(row["remaining_goods"]),
                "added_liters": _format_number(row["added_liters"]),
            }
            for row in rows_data
        ],
        "totals_row": {
            "date_label": "ИТОГО",
            "liters": _format_number(totals["liters"]),
            "amount": _format_number(totals["amount"]),
            "payments": _format_number(totals["payments"]),
            "debts": _format_number(totals["debts"]),
            "average_price": "—",
            "remaining_goods": "—",
            "added_liters": "—",
        },
        "error_message": error_message,
    }


def _build_report_payload(report_type, start_date, end_date):
    if report_type == "sales":
        return _build_sales_report_payload(start_date, end_date)
    if report_type == "payments":
        return _build_payments_report_payload(start_date, end_date)
    if report_type == "cash":
        return _build_cash_report_payload(start_date, end_date)
    if report_type == "turnover":
        return _build_turnover_report_payload(start_date, end_date)
    return None


def _report_excel_file(report_type, month_start, payload):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Отчет"
    columns = payload.get("columns", [])
    rows = payload.get("rows", [])
    totals_row = payload.get("totals_row")
    total_columns = max(1, len(columns))

    title = f"{REPORT_TYPE_LABELS[report_type]} — {_month_label(month_start)}"
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=total_columns)
    title_cell = sheet.cell(row=1, column=1, value=title)
    title_cell.font = Font(bold=True, size=14)
    title_cell.alignment = Alignment(horizontal="center")

    header_fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
    thin_side = Side(style="thin", color="000000")
    border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)

    header_row_index = 3
    for idx, column in enumerate(columns, start=1):
        cell = sheet.cell(row=header_row_index, column=idx, value=column["label"])
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
        cell.border = border

    row_index = header_row_index + 1
    for row in rows:
        for col_idx, column in enumerate(columns, start=1):
            cell = sheet.cell(row=row_index, column=col_idx, value=row.get(column["key"], ""))
            cell.border = border
        row_index += 1

    if totals_row:
        for col_idx, column in enumerate(columns, start=1):
            cell = sheet.cell(row=row_index, column=col_idx, value=totals_row.get(column["key"], ""))
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
            cell.border = border

    for col_idx in range(1, total_columns + 1):
        max_length = 0
        for row_cells in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=col_idx, max_col=col_idx):
            value = row_cells[0].value
            if value is None:
                continue
            max_length = max(max_length, len(str(value)))
        sheet.column_dimensions[get_column_letter(col_idx)].width = min(max_length + 2, 60)

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"Отчет_{REPORT_TYPE_LABELS[report_type]}_{_month_label_filename(month_start)}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/sales-journal")
@login_required
def sales_journal():
    start_date, end_date, _ = _month_bounds()
    start_dt, end_dt = _month_datetime_bounds(start_date, end_date)
    q = request.args.get("q", "").strip()
    query = (
        Sale.query
        .options(joinedload(Sale.car).joinedload(Car.client))
        .join(Sale.car)
        .join(Car.client)
        .filter(Sale.created_at >= start_dt, Sale.created_at < end_dt)
    )
    if q:
        query = query.filter(
            db.or_(
                Client.fio.ilike(f"%{q}%"),
                Car.number.ilike(f"%{q}%"),
            )
        )
    all_sales = query.order_by(Sale.created_at.desc()).all()
    return render_template("sales_journal.html", sales=all_sales, q=q)


@app.route("/debts-journal")
@login_required
def debts_journal():
    q = request.args.get("q", "").strip()
    client_id = request.args.get("client_id", "").strip()
    query = (
        Sale.query
        .options(joinedload(Sale.car).joinedload(Car.client))
        .join(Sale.car)
        .join(Car.client)
        .filter((Sale.total - db.func.coalesce(Sale.payment_amount, 0.0)) > 0)
    )
    if q:
        query = query.filter(
            db.or_(
                Client.fio.ilike(f"%{q}%"),
                Car.number.ilike(f"%{q}%"),
            )
        )
    if client_id:
        try:
            query = query.filter(Client.id == int(client_id))
        except ValueError:
            pass
    all_debts = query.order_by(Sale.created_at.desc()).all()
    return render_template("debts_journal.html", sales=all_debts, q=q, client_id=client_id)


@app.route("/debts-by-client")
@admin_required
def debts_by_client():
    q = request.args.get("q", "").strip()
    debt_sales = (
        Sale.query
        .options(joinedload(Sale.car).joinedload(Car.client))
        .join(Sale.car)
        .join(Car.client)
        .filter((Sale.total - db.func.coalesce(Sale.payment_amount, 0.0)) > 0)
        .all()
    )

    clients_map = {}
    for sale in debt_sales:
        client = sale.car.client
        if client.id not in clients_map:
            clients_map[client.id] = {
                "client": client,
                "count": 0,
                "total_debt": 0.0,
                "total_paid": 0.0,
            }
        clients_map[client.id]["count"] += 1
        clients_map[client.id]["total_debt"] += sale.total
        clients_map[client.id]["total_paid"] += sale.payment_amount or 0.0

    rows = []
    for data in clients_map.values():
        data["remaining"] = data["total_debt"] - data["total_paid"]
        rows.append(data)

    if q:
        rows = [r for r in rows if q.lower() in r["client"].fio.lower()]

    rows.sort(key=lambda x: x["remaining"], reverse=True)

    totals = {
        "count": sum(r["count"] for r in rows),
        "total_debt": sum(r["total_debt"] for r in rows),
        "total_paid": sum(r["total_paid"] for r in rows),
        "remaining": sum(r["remaining"] for r in rows),
    }

    return render_template("debts_by_client.html", rows=rows, totals=totals, q=q)


@app.route("/pay-debt/<int:sale_id>", methods=["POST"])
@login_required
def pay_debt(sale_id):
    sale = Sale.query.options(joinedload(Sale.car).joinedload(Car.client)).get_or_404(sale_id)
    amount_str = request.form.get("amount", "").strip()
    try:
        amount = float(amount_str)
        if amount <= 0:
            raise ValueError
    except (ValueError, TypeError):
        return redirect(url_for("debts_journal"))

    payment_method = request.form.get("payment_method", "").strip()
    if payment_method not in ["наличка", "безнал", "доллар"]:
        return redirect(url_for("debts_journal"))

    remaining = sale.total - (sale.payment_amount or 0.0)
    if amount > remaining:
        amount = remaining

    sale.payment_amount = round((sale.payment_amount or 0.0) + amount, 2)
    if sale.payment_amount >= round(sale.total, 2):
        sale.payment_method = payment_method

    payment = Payment(
        client_id=sale.car.client_id,
        sale_id=sale.id,
        amount=amount,
        payment_type="долг",
        payment_method=payment_method,
    )
    db.session.add(payment)
    db.session.commit()
    return redirect(url_for("debts_journal"))


@app.route("/payments")
@login_required
def payments():
    start_date, end_date, _ = _month_bounds()
    start_dt, end_dt = _month_datetime_bounds(start_date, end_date)
    q = request.args.get("q", "").strip()
    query = (
        Payment.query
        .join(Payment.client)
        .filter(Payment.created_at >= start_dt, Payment.created_at < end_dt)
        .order_by(Payment.created_at.desc())
    )
    if q:
        query = query.filter(Client.fio.ilike(f"%{q}%"))
    all_payments = query.all()
    return render_template("payments.html", payments=all_payments, q=q)


@app.route("/cash")
@login_required
def cash():
    start_date, end_date, _ = _month_bounds()
    rows = _build_cash_rows(start_date, end_date)
    return render_template("cash.html", rows=rows)


@app.route("/reports")
@login_required
def reports():
    _, _, default_month = _month_bounds()
    return render_template(
        "reports.html",
        default_month=default_month,
        report_type_labels=REPORT_TYPE_LABELS,
        can_view_turnover=session.get("role") == "admin",
    )


@app.route("/api/reports")
@login_required
def api_reports():
    report_type = request.args.get("report_type", "").strip()
    month_value = request.args.get("month", "").strip()
    start_date, end_date, normalized_month = _month_bounds(month_value)
    if report_type not in REPORT_TYPE_LABELS:
        return jsonify({"error": "Неверный тип отчета."}), 400
    if report_type == "turnover" and session.get("role") != "admin":
        return jsonify({"error": "Доступ запрещен."}), 403

    payload = _build_report_payload(report_type, start_date, end_date)
    response = {
        "report_type": report_type,
        "report_label": REPORT_TYPE_LABELS[report_type],
        "month": normalized_month,
        "month_label": _month_label(start_date),
        "columns": payload.get("columns", []),
        "rows": payload.get("rows", []),
    }
    if payload.get("totals_row"):
        response["totals_row"] = payload["totals_row"]
    if payload.get("error_message"):
        response["error"] = payload["error_message"]
    return jsonify(response)


@app.route("/api/export-report")
@login_required
def export_report():
    report_type = request.args.get("report_type", "").strip()
    month_value = request.args.get("month", "").strip()
    start_date, end_date, _ = _month_bounds(month_value)
    if report_type not in REPORT_TYPE_LABELS:
        return jsonify({"error": "Неверный тип отчета."}), 400
    if report_type == "turnover" and session.get("role") != "admin":
        return jsonify({"error": "Доступ запрещен."}), 403
    payload = _build_report_payload(report_type, start_date, end_date)
    return _report_excel_file(report_type, start_date, payload)


@app.route("/turnover")
@admin_required
def turnover():
    start_date, end_date, _ = _month_bounds()
    rows_data, totals, error_message = _build_turnover_rows(start_date, end_date)

    return render_template(
        "turnover.html",
        rows=rows_data,
        totals=totals,
        error_message=error_message,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
    )


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        _ensure_car_stock_column()
        _ensure_daily_stock_tables()

        legacy_users = User.query.all()
        for legacy_user in legacy_users:
            if not (legacy_user.password or "").startswith(("pbkdf2:", "scrypt:")):
                legacy_user.password = generate_password_hash(legacy_user.password or "")

        admin_password = os.getenv("DEFAULT_ADMIN_PASSWORD") or "admin123"
        operator_password = os.getenv("DEFAULT_OPERATOR_PASSWORD") or "operator123"

        if not User.query.filter_by(username="admin").first():
            admin = User(username="admin", password=generate_password_hash(admin_password), role="admin")
            db.session.add(admin)
            print("✅ Создан пользователь: admin")

        if not User.query.filter_by(username="operator").first():
            operator = User(username="operator", password=generate_password_hash(operator_password), role="operator")
            db.session.add(operator)
            print("✅ Создан пользователь: operator")

        db.session.commit()

    app.run(debug=os.getenv("FLASK_DEBUG", "False") == "True")
