from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    vehicle_number = db.Column(db.String(50), nullable=False, unique=True)
    full_name = db.Column(db.String(150), nullable=False, unique=True)
    phone_number = db.Column(db.String(10), nullable=False)
    vehicle_brand = db.Column(db.String(100), nullable=False)
    vehicle_color = db.Column(db.String(100), nullable=False)
    inn = db.Column(db.String(14), nullable=False)
    notes = db.Column(db.String(255), nullable=True)


@app.route("/")
def index():
    return redirect(url_for("add_client"))


@app.route("/add-client", methods=["GET", "POST"])
def add_client():
    if request.method == "POST":
        vehicle_number = request.form.get("vehicle_number", "").strip()
        full_name = request.form.get("full_name", "").strip()
        phone_number = "".join(filter(str.isdigit, request.form.get("phone_number", "")))[:10]
        vehicle_brand = request.form.get("vehicle_brand", "").strip()
        vehicle_color = request.form.get("vehicle_color", "").strip()
        inn = "".join(filter(str.isdigit, request.form.get("inn", "")))[:14]
        notes = request.form.get("notes", "").strip()

        is_duplicate = (
            Client.query.filter(
                or_(
                    Client.vehicle_number == vehicle_number,
                    Client.full_name == full_name,
                )
            ).first()
            is not None
        )
        if (
            not vehicle_number
            or not full_name
            or not vehicle_brand
            or not vehicle_color
            or len(phone_number) != 10
            or len(inn) != 14
            or is_duplicate
        ):
            return redirect(url_for("add_client"))

        client = Client(
            vehicle_number=vehicle_number,
            full_name=full_name,
            phone_number=phone_number,
            vehicle_brand=vehicle_brand,
            vehicle_color=vehicle_color,
            inn=inn,
            notes=notes,
        )
        db.session.add(client)
        db.session.commit()
        return redirect(url_for("clients"))


    return render_template("add_client.html")


@app.route("/clients", methods=["GET"])
def clients():
    all_clients = Client.query.order_by(Client.id.desc()).all()
    return render_template("clients.html", clients=all_clients)


@app.route("/client/edit/<int:id>", methods=["GET", "POST"])
def edit_client(id):
    client = Client.query.get_or_404(id)
    if request.method == "POST":
        vehicle_number = request.form.get("vehicle_number", "").strip()
        full_name = request.form.get("full_name", "").strip()
        phone_number = "".join(filter(str.isdigit, request.form.get("phone_number", "")))[:10]
        vehicle_brand = request.form.get("vehicle_brand", "").strip()
        vehicle_color = request.form.get("vehicle_color", "").strip()
        inn = "".join(filter(str.isdigit, request.form.get("inn", "")))[:14]
        notes = request.form.get("notes", "").strip()

        has_duplicate = (
            Client.query.filter(
                Client.id != id,
                or_(
                    Client.vehicle_number == vehicle_number,
                    Client.full_name == full_name,
                ),
            ).first()
            is not None
        )
        if (
            not vehicle_number
            or not full_name
            or not vehicle_brand
            or not vehicle_color
            or len(phone_number) != 10
            or len(inn) != 14
            or has_duplicate
        ):
            return redirect(url_for("edit_client", id=id))

        client.vehicle_number = vehicle_number
        client.full_name = full_name
        client.phone_number = phone_number
        client.vehicle_brand = vehicle_brand
        client.vehicle_color = vehicle_color
        client.inn = inn
        client.notes = notes
        db.session.commit()
        return redirect(url_for("clients"))

    return render_template("edit_client.html", client=client)


@app.route("/client/delete/<int:id>", methods=["POST"])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


@app.route("/api/check-unique", methods=["GET"])
def check_unique():
    field = request.args.get("field", "").strip()
    value = request.args.get("value", "").strip()
    exclude_id = request.args.get("exclude_id", type=int)

    field_map = {
        "vehicle_number": Client.vehicle_number,
        "full_name": Client.full_name,
    }
    model_field = field_map.get(field)
    if model_field is None or not value:
        return jsonify({"unique": True})

    query = Client.query.filter(model_field == value)
    if exclude_id is not None:
        query = query.filter(Client.id != exclude_id)

    return jsonify({"unique": query.first() is None})


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run()
