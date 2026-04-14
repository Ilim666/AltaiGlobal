import re

from flask import Flask, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    vehicle_number = db.Column(db.String(50), nullable=False, unique=True)
    full_name = db.Column(db.String(255), nullable=False, unique=True)
    phone_number = db.Column(db.String(10), nullable=False)
    vehicle_brand = db.Column(db.String(100), nullable=False)
    vehicle_color = db.Column(db.String(100), nullable=False)
    inn = db.Column(db.String(14), nullable=False)
    notes = db.Column(db.Text, nullable=True)

    @property
    def formatted_phone(self):
        if self.phone_number and self.phone_number.isdigit() and len(self.phone_number) == 10:
            return f"{self.phone_number[:4]}-{self.phone_number[4:7]}-{self.phone_number[7:]}"
        return self.phone_number


def validate_client_data(form_data, current_client_id=None):
    errors = []
    cleaned = {
        "vehicle_number": form_data.get("vehicle_number", "").strip(),
        "full_name": form_data.get("full_name", "").strip(),
        "phone_number": form_data.get("phone_number", "").strip(),
        "vehicle_brand": form_data.get("vehicle_brand", "").strip(),
        "vehicle_color": form_data.get("vehicle_color", "").strip(),
        "inn": form_data.get("inn", "").strip(),
        "notes": form_data.get("notes", "").strip(),
    }

    required_fields = {
        "vehicle_number": "Номер машины",
        "full_name": "ФИО",
        "phone_number": "Номер телефона",
        "vehicle_brand": "Марка машины",
        "vehicle_color": "Цвет машины",
        "inn": "ИНН",
    }
    for key, label in required_fields.items():
        if not cleaned[key]:
            errors.append(f"Поле «{label}» обязательно для заполнения.")

    if cleaned["phone_number"] and not re.fullmatch(r"\d{10}", cleaned["phone_number"]):
        errors.append("Номер телефона должен содержать только цифры и быть длиной ровно 10 символов.")

    if cleaned["inn"] and not re.fullmatch(r"\d{14}", cleaned["inn"]):
        errors.append("ИНН должен содержать только цифры и быть длиной ровно 14 символов.")

    if cleaned["vehicle_number"]:
        vehicle_number_query = Client.query.filter_by(vehicle_number=cleaned["vehicle_number"])
        if current_client_id is not None:
            vehicle_number_query = vehicle_number_query.filter(Client.id != current_client_id)
        if vehicle_number_query.first():
            errors.append("Номер машины уже существует. Укажите уникальный номер.")

    if cleaned["full_name"]:
        full_name_query = Client.query.filter_by(full_name=cleaned["full_name"])
        if current_client_id is not None:
            full_name_query = full_name_query.filter(Client.id != current_client_id)
        if full_name_query.first():
            errors.append("ФИО уже существует. Укажите уникальное ФИО.")

    return cleaned, errors


@app.route("/")
def index():
    return redirect(url_for("add_client"))


@app.route("/add-client", methods=["GET", "POST"])
def add_client():
    if request.method == "POST":
        form_data, errors = validate_client_data(request.form)
        if errors:
            return render_template("add_client.html", errors=errors, form_data=form_data), 400

        client = Client(
            vehicle_number=form_data["vehicle_number"],
            full_name=form_data["full_name"],
            phone_number=form_data["phone_number"],
            vehicle_brand=form_data["vehicle_brand"],
            vehicle_color=form_data["vehicle_color"],
            inn=form_data["inn"],
            notes=form_data["notes"] or None,
        )
        db.session.add(client)
        db.session.commit()
        return redirect(url_for("clients"))


    return render_template("add_client.html", form_data={})


@app.route("/clients", methods=["GET"])
def clients():
    all_clients = Client.query.order_by(Client.id.desc()).all()
    return render_template("clients.html", clients=all_clients)


@app.route("/client/edit/<int:id>", methods=["GET", "POST"])
def edit_client(id):
    client = Client.query.get_or_404(id)
    if request.method == "POST":
        form_data, errors = validate_client_data(request.form, current_client_id=id)
        if errors:
            return render_template("edit_client.html", client=client, errors=errors, form_data=form_data), 400

        client.vehicle_number = form_data["vehicle_number"]
        client.full_name = form_data["full_name"]
        client.phone_number = form_data["phone_number"]
        client.vehicle_brand = form_data["vehicle_brand"]
        client.vehicle_color = form_data["vehicle_color"]
        client.inn = form_data["inn"]
        client.notes = form_data["notes"] or None
        db.session.commit()
        return redirect(url_for("clients"))

    form_data = {
        "vehicle_number": client.vehicle_number,
        "full_name": client.full_name,
        "phone_number": client.phone_number,
        "vehicle_brand": client.vehicle_brand,
        "vehicle_color": client.vehicle_color,
        "inn": client.inn,
        "notes": client.notes or "",
    }
    return render_template("edit_client.html", client=client, form_data=form_data)


@app.route("/client/delete/<int:id>", methods=["POST"])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


if __name__ == "__main__":
    with app.app_context():
        expected_columns = {"id", "vehicle_number", "full_name", "phone_number", "vehicle_brand", "vehicle_color", "inn", "notes"}
        inspector = inspect(db.engine)
        if inspector.has_table("client"):
            current_columns = {column["name"] for column in inspector.get_columns("client")}
            if current_columns != expected_columns:
                db.drop_all()
        db.create_all()
    app.run()
