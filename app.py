from datetime import datetime

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


class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)


class Sale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    total_price = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)

    client = db.relationship("Client")
    product = db.relationship("Product")


# ── Clients ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("add_client"))


@app.route("/add-client", methods=["GET", "POST"])
def add_client():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        address = request.form.get("address", "").strip()
        if not name or not phone or not address:
            return redirect(url_for("add_client"))

        client = Client(
            name=name,
            phone=phone,
            address=address,
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
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        address = request.form.get("address", "").strip()
        if not name or not phone or not address:
            return redirect(url_for("edit_client", id=id))

        client.name = name
        client.phone = phone
        client.address = address
        db.session.commit()
        return redirect(url_for("clients"))

    return render_template("edit_client.html", client=client)


@app.route("/client/delete/<int:id>", methods=["POST"])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


# ── Products ─────────────────────────────────────────────────────────────────

@app.route("/products")
def products():
    all_products = Product.query.order_by(Product.id.desc()).all()
    return render_template("products.html", products=all_products)


@app.route("/add-product", methods=["GET", "POST"])
def add_product():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price = request.form.get("price", "").strip()
        quantity = request.form.get("quantity", "").strip()
        if not name or not price or not quantity:
            return redirect(url_for("add_product"))

        product = Product(
            name=name,
            price=float(price),
            quantity=int(quantity),
        )
        db.session.add(product)
        db.session.commit()
        return redirect(url_for("products"))

    return render_template("add_product.html")


@app.route("/edit-product/<int:id>", methods=["GET", "POST"])
def edit_product(id):
    product = Product.query.get_or_404(id)
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price = request.form.get("price", "").strip()
        quantity = request.form.get("quantity", "").strip()
        if not name or not price or not quantity:
            return redirect(url_for("edit_product", id=id))

        product.name = name
        product.price = float(price)
        product.quantity = int(quantity)
        db.session.commit()
        return redirect(url_for("products"))

    return render_template("edit_product.html", product=product)


@app.route("/delete-product/<int:id>", methods=["POST"])
def delete_product(id):
    product = Product.query.get_or_404(id)
    db.session.delete(product)
    db.session.commit()
    return redirect(url_for("products"))


# ── Sales ─────────────────────────────────────────────────────────────────────

@app.route("/sales", methods=["GET", "POST"])
def sales():
    all_clients = Client.query.order_by(Client.name).all()
    all_products = Product.query.order_by(Product.name).all()
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        product_id = request.form.get("product_id", "").strip()
        quantity = request.form.get("quantity", "").strip()
        if not client_id or not product_id or not quantity:
            return render_template("sales.html", clients=all_clients, products=all_products)

        product = Product.query.get_or_404(int(product_id))
        qty = int(quantity)
        if qty <= 0 or qty > product.quantity:
            error = f"Недостаточно товара на складе. В наличии: {product.quantity} шт."
            return render_template("sales.html", clients=all_clients, products=all_products, error=error)

        total_price = product.price * qty
        product.quantity -= qty

        sale = Sale(
            client_id=int(client_id),
            product_id=int(product_id),
            quantity=qty,
            total_price=total_price,
        )
        db.session.add(sale)
        db.session.commit()
        return redirect(url_for("sales_list"))

    return render_template("sales.html", clients=all_clients, products=all_products)


@app.route("/sales-list")
def sales_list():
    all_sales = Sale.query.order_by(Sale.id.desc()).all()
    return render_template("sales_list.html", sales=all_sales)


@app.route("/delete-sale/<int:id>", methods=["POST"])
def delete_sale(id):
    sale = Sale.query.get_or_404(id)
    sale.product.quantity += sale.quantity
    db.session.delete(sale)
    db.session.commit()
    return redirect(url_for("sales_list"))


# ── Reports ───────────────────────────────────────────────────────────────────

@app.route("/reports")
def reports():
    return render_template("reports.html")


@app.route("/reports/sales")
def reports_sales():
    all_sales = Sale.query.order_by(Sale.id.desc()).all()
    total = sum(s.total_price for s in all_sales)
    return render_template("reports_sales.html", sales=all_sales, total=total)


@app.route("/reports/clients")
def reports_clients():
    all_clients = Client.query.order_by(Client.name).all()
    data = []
    for client in all_clients:
        client_sales = Sale.query.filter_by(client_id=client.id).all()
        total = sum(s.total_price for s in client_sales)
        data.append({"client": client, "sales_count": len(client_sales), "total": total})
    return render_template("reports_clients.html", data=data)


@app.route("/reports/products")
def reports_products():
    all_products = Product.query.order_by(Product.name).all()
    data = []
    for product in all_products:
        product_sales = Sale.query.filter_by(product_id=product.id).all()
        sold_qty = sum(s.quantity for s in product_sales)
        revenue = sum(s.total_price for s in product_sales)
        data.append({"product": product, "sold_qty": sold_qty, "revenue": revenue})
    return render_template("reports_products.html", data=data)


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run()
