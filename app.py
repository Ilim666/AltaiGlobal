import re
from datetime import datetime, timezone

from flask import Flask, redirect, render_template, request, url_for
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
    all_cars = Car.query.options(joinedload(Car.client)).order_by(Car.id.desc()).all()
    return render_template("cars.html", cars=all_cars)


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


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True)
