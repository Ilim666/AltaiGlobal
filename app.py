import re
from collections import defaultdict
from datetime import datetime, timezone

from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import joinedload

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


def fmt_phone(phone):
    """Format a 10-digit phone string as XXXX-XXX-XXX."""
    digits = re.sub(r"\D", "", str(phone or ""))
    if len(digits) == 10:
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return phone or ""


app.jinja_env.filters["fmt_phone"] = fmt_phone


# ── Models ────────────────────────────────────────────────────────────────────

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
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    sales = db.relationship("Sale", backref="car", lazy=True, cascade="all, delete-orphan")


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


# ── Helpers ───────────────────────────────────────────────────────────────────

def validate_phone(phone):
    digits = re.sub(r"\D", "", phone)
    return digits if len(digits) == 10 else None


def validate_inn(inn):
    digits = re.sub(r"\D", "", inn)
    return digits if len(digits) == 14 else None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("clients"))


@app.route("/clients")
def clients():
    all_clients = Client.query.order_by(Client.id.desc()).all()
    return render_template("clients.html", clients=all_clients)


@app.route("/add-client", methods=["GET", "POST"])
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

        inn_digits = validate_inn(form["inn"])
        if inn_digits is None:
            errors["inn"] = "ИНН должен содержать ровно 14 цифр."

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
def client_detail(id):
    client = Client.query.get_or_404(id)
    cars = Car.query.filter_by(client_id=id).order_by(Car.id.desc()).all()
    return render_template("client_detail.html", client=client, cars=cars)


@app.route("/edit-client/<int:id>", methods=["GET", "POST"])
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

        inn_digits = validate_inn(form["inn"])
        if inn_digits is None:
            errors["inn"] = "ИНН должен содержать ровно 14 цифр."

        if not errors:
            client.fio = form["fio"]
            client.phone = phone_digits
            client.inn = inn_digits
            db.session.commit()
            return redirect(url_for("client_detail", id=id))

    return render_template("edit_client.html", client=client, errors=errors, form=form)


@app.route("/delete-client/<int:id>", methods=["POST"])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


@app.route("/add-car", methods=["GET", "POST"])
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
def cars():
    q = request.args.get("q", "").strip()
    query = Car.query.options(joinedload(Car.client)).join(Car.client)
    if q:
        query = query.filter(Client.fio.ilike(f"%{q}%"))
    all_cars = query.order_by(Car.id.desc()).all()
    return render_template("cars.html", cars=all_cars, q=q)


@app.route("/delete-car/<int:id>", methods=["POST"])
def delete_car(id):
    car = Car.query.get_or_404(id)
    client_id = car.client_id
    next_page = request.form.get("next", "")
    db.session.delete(car)
    db.session.commit()
    if next_page == "cars":
        return redirect(url_for("cars"))
    return redirect(url_for("client_detail", id=client_id))


# ── Sales ─────────────────────────────────────────────────────────────────────

@app.route("/api/car-search")
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


@app.route("/sales", methods=["GET", "POST"])
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


@app.route("/sales-journal")
def sales_journal():
    q = request.args.get("q", "").strip()
    query = (
        Sale.query
        .options(joinedload(Sale.car).joinedload(Car.client))
        .join(Sale.car)
        .join(Car.client)
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


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True)
