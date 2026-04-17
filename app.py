from datetime import datetime

from flask import Flask, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# ── Models ────────────────────────────────────────────────────────────────────

class Client(db.Model):
    __tablename__ = "clients"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    inn = db.Column(db.String(20), nullable=False, default="")
    address = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    sales = db.relationship("Sale", backref="client", lazy=True)


class Product(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    sales = db.relationship("Sale", backref="product", lazy=True)


class Sale(db.Model):
    __tablename__ = "sales"
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("clients.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    total_price = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ── Home ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    total_clients = Client.query.count()
    total_products = Product.query.count()
    total_sales = Sale.query.count()
    total_revenue = db.session.query(func.sum(Sale.total_price)).scalar() or 0
    recent_sales = (
        Sale.query.order_by(Sale.created_at.desc()).limit(5).all()
    )
    return render_template(
        "index.html",
        total_clients=total_clients,
        total_products=total_products,
        total_sales=total_sales,
        total_revenue=total_revenue,
        recent_sales=recent_sales,
    )


# ── Clients ───────────────────────────────────────────────────────────────────

@app.route("/clients")
def clients():
    search = request.args.get("q", "").strip()
    query = Client.query
    if search:
        query = query.filter(Client.name.ilike(f"%{search}%"))
    all_clients = query.order_by(Client.created_at.desc()).all()
    return render_template("clients.html", clients=all_clients, search=search)


@app.route("/add-client", methods=["GET", "POST"])
def add_client():
    error = None
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        inn = request.form.get("inn", "").strip()
        address = request.form.get("address", "").strip()
        if not name or not phone or not address:
            error = "Заполните обязательные поля: Имя, Телефон, Адрес."
        else:
            db.session.add(Client(name=name, phone=phone, inn=inn, address=address))
            db.session.commit()
            return redirect(url_for("clients"))
    return render_template("add_client.html", error=error)


@app.route("/edit-client/<int:id>", methods=["GET", "POST"])
def edit_client(id):
    client = db.get_or_404(Client, id)
    error = None
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        inn = request.form.get("inn", "").strip()
        address = request.form.get("address", "").strip()
        if not name or not phone or not address:
            error = "Заполните обязательные поля: Имя, Телефон, Адрес."
        else:
            client.name = name
            client.phone = phone
            client.inn = inn
            client.address = address
            db.session.commit()
            return redirect(url_for("clients"))
    return render_template("edit_client.html", client=client, error=error)


@app.route("/delete-client/<int:id>", methods=["POST"])
def delete_client(id):
    client = db.get_or_404(Client, id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for("clients"))


# ── Products ──────────────────────────────────────────────────────────────────

@app.route("/products")
def products():
    search = request.args.get("q", "").strip()
    query = Product.query
    if search:
        query = query.filter(Product.name.ilike(f"%{search}%"))
    all_products = query.order_by(Product.created_at.desc()).all()
    return render_template("products.html", products=all_products, search=search)


@app.route("/add-product", methods=["GET", "POST"])
def add_product():
    error = None
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price_str = request.form.get("price", "").strip()
        qty_str = request.form.get("quantity", "0").strip()
        if not name or not price_str:
            error = "Заполните обязательные поля: Название, Цена."
        else:
            try:
                price = float(price_str)
                qty = int(qty_str) if qty_str else 0
                if price < 0 or qty < 0:
                    raise ValueError
            except ValueError:
                error = "Цена и количество должны быть неотрицательными числами."
            else:
                db.session.add(Product(name=name, price=price, quantity=qty))
                db.session.commit()
                return redirect(url_for("products"))
    return render_template("add_product.html", error=error)


@app.route("/edit-product/<int:id>", methods=["GET", "POST"])
def edit_product(id):
    product = db.get_or_404(Product, id)
    error = None
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price_str = request.form.get("price", "").strip()
        qty_str = request.form.get("quantity", "0").strip()
        if not name or not price_str:
            error = "Заполните обязательные поля: Название, Цена."
        else:
            try:
                price = float(price_str)
                qty = int(qty_str) if qty_str else 0
                if price < 0 or qty < 0:
                    raise ValueError
            except ValueError:
                error = "Цена и количество должны быть неотрицательными числами."
            else:
                product.name = name
                product.price = price
                product.quantity = qty
                db.session.commit()
                return redirect(url_for("products"))
    return render_template("edit_product.html", product=product, error=error)


@app.route("/delete-product/<int:id>", methods=["POST"])
def delete_product(id):
    product = db.get_or_404(Product, id)
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
        client_id = request.form.get("client_id", "").strip()
        product_id = request.form.get("product_id", "").strip()
        qty_str = request.form.get("quantity", "").strip()
        if not client_id or not product_id or not qty_str:
            error = "Заполните все поля."
        else:
            try:
                qty = int(qty_str)
                if qty <= 0:
                    raise ValueError
            except ValueError:
                error = "Количество должно быть положительным числом."
            else:
                product = db.session.get(Product, int(product_id))
                if not product:
                    error = "Товар не найден."
                elif product.quantity < qty:
                    error = f"Недостаточно товара на складе. Доступно: {product.quantity} шт."
                else:
                    total = round(product.price * qty, 2)
                    product.quantity -= qty
                    db.session.add(
                        Sale(
                            client_id=int(client_id),
                            product_id=int(product_id),
                            quantity=qty,
                            total_price=total,
                        )
                    )
                    db.session.commit()
                    return redirect(url_for("sales_list"))
    return render_template("sales.html", clients=all_clients, products=all_products, error=error)


@app.route("/sales-list")
def sales_list():
    search = request.args.get("q", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    query = Sale.query
    if search:
        query = query.join(Client).filter(Client.name.ilike(f"%{search}%"))
    if date_from:
        try:
            query = query.filter(Sale.created_at >= datetime.strptime(date_from, "%Y-%m-%d"))
        except ValueError:
            pass
    if date_to:
        try:
            query = query.filter(Sale.created_at <= datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59))
        except ValueError:
            pass
    all_sales = query.order_by(Sale.created_at.desc()).all()
    return render_template("sales_list.html", sales=all_sales, search=search, date_from=date_from, date_to=date_to)


@app.route("/delete-sale/<int:id>", methods=["POST"])
def delete_sale(id):
    sale = db.get_or_404(Sale, id)
    # restore product stock
    product = db.session.get(Product, sale.product_id)
    if product:
        product.quantity += sale.quantity
    db.session.delete(sale)
    db.session.commit()
    return redirect(url_for("sales_list"))


# ── Reports ───────────────────────────────────────────────────────────────────

@app.route("/reports")
def reports():
    # Sales by client
    by_client = (
        db.session.query(Client.name, func.sum(Sale.total_price), func.count(Sale.id))
        .join(Sale, Sale.client_id == Client.id)
        .group_by(Client.id)
        .order_by(func.sum(Sale.total_price).desc())
        .all()
    )

    # Sales by product
    by_product = (
        db.session.query(Product.name, func.sum(Sale.quantity), func.sum(Sale.total_price))
        .join(Sale, Sale.product_id == Product.id)
        .group_by(Product.id)
        .order_by(func.sum(Sale.total_price).desc())
        .all()
    )

    # Sales by date (last 30 days, grouped by day)
    by_date = (
        db.session.query(
            func.strftime("%Y-%m-%d", Sale.created_at).label("day"),
            func.sum(Sale.total_price),
            func.count(Sale.id),
        )
        .group_by("day")
        .order_by("day")
        .all()
    )

    return render_template(
        "reports.html",
        by_client=by_client,
        by_product=by_product,
        by_date=by_date,
    )


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=False)
