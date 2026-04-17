from datetime import datetime

from flask import Flask, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    address = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    sales = db.relationship("Sale", backref="client", lazy=True)


class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    sales = db.relationship("Sale", backref="product", lazy=True)


class Sale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    total_price = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ── Clients ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("clients"))


@app.route("/add-client", methods=["GET", "POST"])
def add_client():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        address = request.form.get("address", "").strip()
        if not name or not phone or not address:
            return redirect(url_for("add_client"))

        client = Client(name=name, phone=phone, address=address)
        db.session.add(client)
        db.session.commit()
        return redirect(url_for("clients"))

    return render_template("add_client.html")


@app.route("/clients", methods=["GET"])
def clients():
    all_clients = Client.query.order_by(Client.id.desc()).all()
    return render_template("clients.html", clients=all_clients)


@app.route("/edit-client/<int:id>", methods=["GET", "POST"])
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


@app.route("/delete-client/<int:id>", methods=["POST"])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


# ── Products ──────────────────────────────────────────────────────────────────

@app.route("/products")
def products():
    all_products = Product.query.order_by(Product.id.desc()).all()
    return render_template("products.html", products=all_products)


@app.route("/add-product", methods=["GET", "POST"])
def add_product():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price_str = request.form.get("price", "").strip()
        quantity_str = request.form.get("quantity", "0").strip()
        if not name or not price_str:
            return redirect(url_for("add_product"))
        try:
            price = float(price_str)
            quantity = int(quantity_str)
        except ValueError:
            return redirect(url_for("add_product"))

        product = Product(name=name, price=price, quantity=quantity)
        db.session.add(product)
        db.session.commit()
        return redirect(url_for("products"))

    return render_template("add_product.html")


@app.route("/edit-product/<int:id>", methods=["GET", "POST"])
def edit_product(id):
    product = Product.query.get_or_404(id)
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price_str = request.form.get("price", "").strip()
        quantity_str = request.form.get("quantity", "0").strip()
        if not name or not price_str:
            return redirect(url_for("edit_product", id=id))
        try:
            price = float(price_str)
            quantity = int(quantity_str)
        except ValueError:
            return redirect(url_for("edit_product", id=id))

        product.name = name
        product.price = price
        product.quantity = quantity
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
    error = None

    if request.method == "POST":
        client_id_str = request.form.get("client_id", "").strip()
        product_id_str = request.form.get("product_id", "").strip()
        quantity_str = request.form.get("quantity", "").strip()

        if not client_id_str or not product_id_str or not quantity_str:
            error = "Все поля обязательны."
        else:
            try:
                client_id = int(client_id_str)
                product_id = int(product_id_str)
                quantity = int(quantity_str)
                if quantity <= 0:
                    raise ValueError
            except ValueError:
                error = "Некорректные данные."

            if not error:
                product = Product.query.get_or_404(product_id)
                if product.quantity < quantity:
                    error = f"Недостаточно товара на складе (доступно: {product.quantity})."
                else:
                    total_price = product.price * quantity
                    product.quantity -= quantity
                    sale = Sale(
                        client_id=client_id,
                        product_id=product_id,
                        quantity=quantity,
                        total_price=total_price,
                    )
                    db.session.add(sale)
                    db.session.commit()
                    return redirect(url_for("sales_list"))

    return render_template("sales.html", clients=all_clients, products=all_products, error=error)


@app.route("/sales-list")
def sales_list():
    all_sales = (
        Sale.query
        .order_by(Sale.created_at.desc())
        .all()
    )
    return render_template("sales_list.html", sales=all_sales)


@app.route("/delete-sale/<int:id>", methods=["POST"])
def delete_sale(id):
    sale = Sale.query.get_or_404(id)
    # Restore product stock
    sale.product.quantity += sale.quantity
    db.session.delete(sale)
    db.session.commit()
    return redirect(url_for("sales_list"))


# ── Reports ───────────────────────────────────────────────────────────────────

@app.route("/reports")
def reports():
    total_sales = db.session.query(func.count(Sale.id)).scalar() or 0
    total_revenue = db.session.query(func.sum(Sale.total_price)).scalar() or 0
    total_clients = db.session.query(func.count(Client.id)).scalar() or 0
    total_products = db.session.query(func.count(Product.id)).scalar() or 0
    return render_template(
        "reports.html",
        total_sales=total_sales,
        total_revenue=total_revenue,
        total_clients=total_clients,
        total_products=total_products,
    )


@app.route("/reports/by-client")
def reports_by_client():
    rows = (
        db.session.query(
            Client.name,
            func.count(Sale.id).label("sale_count"),
            func.sum(Sale.total_price).label("total_spent"),
        )
        .join(Sale, Sale.client_id == Client.id)
        .group_by(Client.id)
        .order_by(func.sum(Sale.total_price).desc())
        .all()
    )
    return render_template("reports_by_client.html", rows=rows)


@app.route("/reports/by-product")
def reports_by_product():
    rows = (
        db.session.query(
            Product.name,
            func.sum(Sale.quantity).label("total_qty"),
            func.sum(Sale.total_price).label("total_revenue"),
        )
        .join(Sale, Sale.product_id == Product.id)
        .group_by(Product.id)
        .order_by(func.sum(Sale.total_price).desc())
        .all()
    )
    return render_template("reports_by_product.html", rows=rows)


@app.route("/reports/by-date")
def reports_by_date():
    rows = (
        db.session.query(
            func.date(Sale.created_at).label("sale_date"),
            func.count(Sale.id).label("sale_count"),
            func.sum(Sale.total_price).label("total_revenue"),
        )
        .group_by(func.date(Sale.created_at))
        .order_by(func.date(Sale.created_at).desc())
        .all()
    )
    return render_template("reports_by_date.html", rows=rows)


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run()
