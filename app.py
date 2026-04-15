from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(200), nullable=False, unique=True)
    phone_number = db.Column(db.String(10), nullable=False)
    inn = db.Column(db.String(14), nullable=False)
    vehicles = db.relationship("Vehicle", backref="client", lazy=True, cascade="all, delete-orphan")


class Vehicle(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    vehicle_number = db.Column(db.String(20), nullable=False, unique=True)
    vehicle_brand = db.Column(db.String(100), nullable=False)
    vehicle_color = db.Column(db.String(50), nullable=False)
    notes = db.Column(db.String(500), nullable=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)


@app.route("/")
def index():
    return redirect(url_for("vehicles"))


@app.route("/add-client", methods=["GET", "POST"])
def add_client():
    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        phone_number = request.form.get("phone_number", "").strip()
        inn = request.form.get("inn", "").strip()
        vehicle_number = request.form.get("vehicle_number", "").strip()
        vehicle_brand = request.form.get("vehicle_brand", "").strip()
        vehicle_color = request.form.get("vehicle_color", "").strip()
        notes = request.form.get("notes", "").strip()

        if not full_name or not phone_number or not inn or not vehicle_number or not vehicle_brand or not vehicle_color:
            return redirect(url_for("add_client"))

        client = Client(full_name=full_name, phone_number=phone_number, inn=inn)
        db.session.add(client)
        db.session.flush()

        vehicle = Vehicle(
            vehicle_number=vehicle_number,
            vehicle_brand=vehicle_brand,
            vehicle_color=vehicle_color,
            notes=notes if notes else None,
            client_id=client.id,
        )
        db.session.add(vehicle)
        db.session.commit()
        return redirect(url_for("vehicles"))

    return render_template("add_client.html")


@app.route("/add-vehicle", methods=["GET", "POST"])
def add_vehicle():
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        vehicle_number = request.form.get("vehicle_number", "").strip()
        vehicle_brand = request.form.get("vehicle_brand", "").strip()
        vehicle_color = request.form.get("vehicle_color", "").strip()
        notes = request.form.get("notes", "").strip()

        if not client_id or not vehicle_number or not vehicle_brand or not vehicle_color:
            return redirect(url_for("add_vehicle"))

        client = Client.query.get_or_404(int(client_id))
        vehicle = Vehicle(
            vehicle_number=vehicle_number,
            vehicle_brand=vehicle_brand,
            vehicle_color=vehicle_color,
            notes=notes if notes else None,
            client_id=client.id,
        )
        db.session.add(vehicle)
        db.session.commit()
        return redirect(url_for("vehicles"))

    return render_template("add_vehicle.html")


@app.route("/vehicles", methods=["GET"])
def vehicles():
    all_vehicles = (
        db.session.query(Vehicle)
        .join(Client)
        .order_by(Vehicle.id.desc())
        .all()
    )
    return render_template("vehicles.html", vehicles=all_vehicles)


@app.route("/vehicle/edit/<int:id>", methods=["GET", "POST"])
def edit_vehicle(id):
    vehicle = Vehicle.query.get_or_404(id)
    if request.method == "POST":
        vehicle_number = request.form.get("vehicle_number", "").strip()
        vehicle_brand = request.form.get("vehicle_brand", "").strip()
        vehicle_color = request.form.get("vehicle_color", "").strip()
        notes = request.form.get("notes", "").strip()

        if not vehicle_number or not vehicle_brand or not vehicle_color:
            return redirect(url_for("edit_vehicle", id=id))

        vehicle.vehicle_number = vehicle_number
        vehicle.vehicle_brand = vehicle_brand
        vehicle.vehicle_color = vehicle_color
        vehicle.notes = notes if notes else None
        db.session.commit()
        return redirect(url_for("vehicles"))

    return render_template("edit_vehicle.html", vehicle=vehicle)


@app.route("/vehicle/delete/<int:id>", methods=["POST"])
def delete_vehicle(id):
    vehicle = Vehicle.query.get_or_404(id)
    db.session.delete(vehicle)
    db.session.commit()
    return redirect(url_for("vehicles"))


@app.route("/api/check-unique")
def check_unique():
    field = request.args.get("field", "")
    value = request.args.get("value", "").strip()
    exclude_id = request.args.get("exclude_id", type=int)

    if field == "full_name":
        query = Client.query.filter(Client.full_name == value)
        if exclude_id:
            query = query.filter(Client.id != exclude_id)
        unique = query.count() == 0
    elif field == "vehicle_number":
        query = Vehicle.query.filter(Vehicle.vehicle_number == value)
        if exclude_id:
            query = query.filter(Vehicle.id != exclude_id)
        unique = query.count() == 0
    else:
        unique = True

    return jsonify({"unique": unique})


@app.route("/api/search-clients")
def search_clients():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"clients": []})

    results = Client.query.filter(
        db.or_(
            Client.full_name.ilike(f"%{q}%"),
            Client.phone_number.ilike(f"%{q}%"),
        )
    ).limit(10).all()

    clients = [
        {"id": c.id, "full_name": c.full_name, "phone_number": c.phone_number, "inn": c.inn}
        for c in results
    ]
    return jsonify({"clients": clients})


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run()
