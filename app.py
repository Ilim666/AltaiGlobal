from flask import Flask, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    address = db.Column(db.String(255), nullable=False)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/clients", methods=["GET", "POST"])
def clients():
    if request.method == "POST":
        client = Client(
            name=request.form["name"].strip(),
            phone=request.form["phone"].strip(),
            address=request.form["address"].strip(),
        )
        db.session.add(client)
        db.session.commit()
        return redirect(url_for("clients"))

    all_clients = Client.query.order_by(Client.id.desc()).all()
    return render_template("clients.html", clients=all_clients)


@app.route("/client/edit/<int:id>", methods=["GET", "POST"])
def edit_client(id):
    client = Client.query.get_or_404(id)
    if request.method == "POST":
        client.name = request.form["name"].strip()
        client.phone = request.form["phone"].strip()
        client.address = request.form["address"].strip()
        db.session.commit()
        return redirect(url_for("clients"))

    return render_template("edit_client.html", client=client)


@app.route("/client/delete/<int:id>")
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True)
