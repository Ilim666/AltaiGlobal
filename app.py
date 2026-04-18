import re
from datetime import datetime

from flask import Flask, flash, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = "altai-secret-key"
db = SQLAlchemy(app)


# ── Models ────────────────────────────────────────────────────────────────────

class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fio = db.Column(db.String(100), nullable=False, unique=True)
    phone = db.Column(db.String(10), nullable=False)
    inn = db.Column(db.String(14), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    cars = db.relationship("Car", backref="client", lazy=True, cascade="all, delete-orphan")


class Car(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    number = db.Column(db.String(20), nullable=False, unique=True)
    brand = db.Column(db.String(100), nullable=False)
    color = db.Column(db.String(50), nullable=False)
    note = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ── Validation helpers ────────────────────────────────────────────────────────

def validate_phone(phone):
    return bool(re.fullmatch(r"\d{10}", phone))


def validate_inn(inn):
    return bool(re.fullmatch(r"\d{14}", inn))


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
        fio = request.form.get("fio", "").strip()
        phone = request.form.get("phone", "").strip()
        inn = request.form.get("inn", "").strip()
        car_number = request.form.get("car_number", "").strip()
        car_brand = request.form.get("car_brand", "").strip()
        car_color = request.form.get("car_color", "").strip()
        car_note = request.form.get("car_note", "").strip()

        form = {
            "fio": fio, "phone": phone, "inn": inn,
            "car_number": car_number, "car_brand": car_brand,
            "car_color": car_color, "car_note": car_note,
        }

        if not fio:
            errors["fio"] = "ФИО обязательно"
        elif Client.query.filter_by(fio=fio).first():
            errors["fio"] = "Клиент с таким ФИО уже существует"

        if not validate_phone(phone):
            errors["phone"] = "Телефон должен содержать ровно 10 цифр"

        if not validate_inn(inn):
            errors["inn"] = "ИНН должен содержать ровно 14 цифр"

        if not car_number:
            errors["car_number"] = "Номер машины обязателен"
        elif Car.query.filter_by(number=car_number).first():
            errors["car_number"] = "Машина с таким номером уже существует"

        if not car_brand:
            errors["car_brand"] = "Марка машины обязательна"

        if not car_color:
            errors["car_color"] = "Цвет машины обязателен"

        if not errors:
            client = Client(fio=fio, phone=phone, inn=inn)
            db.session.add(client)
            db.session.flush()
            car = Car(
                client_id=client.id,
                number=car_number,
                brand=car_brand,
                color=car_color,
                note=car_note or None,
            )
            db.session.add(car)
            db.session.commit()
            flash("Клиент и машина успешно добавлены!", "success")
            return redirect(url_for("client_detail", id=client.id))

    return render_template("add_client.html", errors=errors, form=form)


@app.route("/client/<int:id>")
def client_detail(id):
    client = Client.query.get_or_404(id)
    return render_template("client_detail.html", client=client)


@app.route("/edit-client/<int:id>", methods=["GET", "POST"])
def edit_client(id):
    client = Client.query.get_or_404(id)
    errors = {}

    if request.method == "POST":
        fio = request.form.get("fio", "").strip()
        phone = request.form.get("phone", "").strip()
        inn = request.form.get("inn", "").strip()

        if not fio:
            errors["fio"] = "ФИО обязательно"
        elif fio != client.fio and Client.query.filter_by(fio=fio).first():
            errors["fio"] = "Клиент с таким ФИО уже существует"

        if not validate_phone(phone):
            errors["phone"] = "Телефон должен содержать ровно 10 цифр"

        if not validate_inn(inn):
            errors["inn"] = "ИНН должен содержать ровно 14 цифр"

        if not errors:
            client.fio = fio
            client.phone = phone
            client.inn = inn
            db.session.commit()
            flash("Данные клиента обновлены!", "success")
            return redirect(url_for("client_detail", id=client.id))

    return render_template("edit_client.html", client=client, errors=errors)


@app.route("/delete-client/<int:id>", methods=["POST"])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    flash("Клиент удалён!", "info")
    return redirect(url_for("clients"))


@app.route("/add-car", methods=["GET", "POST"])
def add_car():
    all_clients = Client.query.order_by(Client.fio).all()
    errors = {}
    form = {}

    if request.method == "POST":
        client_id_str = request.form.get("client_id", "").strip()
        car_number = request.form.get("car_number", "").strip()
        car_brand = request.form.get("car_brand", "").strip()
        car_color = request.form.get("car_color", "").strip()
        car_note = request.form.get("car_note", "").strip()

        form = {
            "client_id": client_id_str,
            "car_number": car_number, "car_brand": car_brand,
            "car_color": car_color, "car_note": car_note,
        }

        if not client_id_str:
            errors["client_id"] = "Выберите клиента"

        if not car_number:
            errors["car_number"] = "Номер машины обязателен"
        elif Car.query.filter_by(number=car_number).first():
            errors["car_number"] = "Машина с таким номером уже существует"

        if not car_brand:
            errors["car_brand"] = "Марка машины обязательна"

        if not car_color:
            errors["car_color"] = "Цвет машины обязателен"

        if not errors:
            try:
                client_id = int(client_id_str)
            except (ValueError, TypeError):
                errors["client_id"] = "Некорректный клиент"

        if not errors:
            car = Car(
                client_id=client_id,
                number=car_number,
                brand=car_brand,
                color=car_color,
                note=car_note or None,
            )
            db.session.add(car)
            db.session.commit()
            flash("Машина успешно добавлена!", "success")
            return redirect(url_for("client_detail", id=client_id))

    preselect = request.args.get("client_id")
    return render_template("add_car.html", clients=all_clients, errors=errors, form=form, preselect=preselect)


@app.route("/edit-car/<int:id>", methods=["GET", "POST"])
def edit_car(id):
    car = Car.query.get_or_404(id)
    errors = {}

    if request.method == "POST":
        car_number = request.form.get("car_number", "").strip()
        car_brand = request.form.get("car_brand", "").strip()
        car_color = request.form.get("car_color", "").strip()
        car_note = request.form.get("car_note", "").strip()

        if not car_number:
            errors["car_number"] = "Номер машины обязателен"
        elif car_number != car.number and Car.query.filter_by(number=car_number).first():
            errors["car_number"] = "Машина с таким номером уже существует"

        if not car_brand:
            errors["car_brand"] = "Марка машины обязательна"

        if not car_color:
            errors["car_color"] = "Цвет машины обязателен"

        if not errors:
            car.number = car_number
            car.brand = car_brand
            car.color = car_color
            car.note = car_note or None
            db.session.commit()
            flash("Данные машины обновлены!", "success")
            return redirect(url_for("client_detail", id=car.client_id))

    return render_template("edit_car.html", car=car, errors=errors)


@app.route("/delete-car/<int:id>", methods=["POST"])
def delete_car(id):
    car = Car.query.get_or_404(id)
    client_id = car.client_id
    db.session.delete(car)
    db.session.commit()
    flash("Машина удалена!", "info")
    return redirect(url_for("client_detail", id=client_id))


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run()
