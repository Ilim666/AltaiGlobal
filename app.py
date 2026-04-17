import csv
import io
from datetime import datetime

from flask import Flask, jsonify, render_template, request, Response
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///altai.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Client(db.Model):
    __tablename__ = "clients"

    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(200), nullable=False)
    phone = db.Column(db.String(30), nullable=False)
    inn = db.Column(db.String(20), nullable=True, default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    sales = db.relationship("Sale", backref="client", lazy=True)

    def to_dict(self):
        return {
            "id": self.id,
            "full_name": self.full_name,
            "phone": self.phone,
            "inn": self.inn or "",
            "created_at": self.created_at.strftime("%d.%m.%Y %H:%M") if self.created_at else "",
        }


class Product(db.Model):
    __tablename__ = "products"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    price = db.Column(db.Float, nullable=False, default=0.0)
    quantity = db.Column(db.Integer, nullable=False, default=0)

    sales = db.relationship("Sale", backref="product", lazy=True)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "price": self.price,
            "quantity": self.quantity,
        }


class Sale(db.Model):
    __tablename__ = "sales"

    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("clients.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=1)
    total = db.Column(db.Float, nullable=False, default=0.0)
    date = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "client_id": self.client_id,
            "client_name": self.client.full_name if self.client else "",
            "product_id": self.product_id,
            "product_name": self.product.name if self.product else "",
            "product_price": self.product.price if self.product else 0,
            "quantity": self.quantity,
            "total": self.total,
            "date": self.date.strftime("%d.%m.%Y %H:%M") if self.date else "",
        }


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/clients")
def clients_page():
    return render_template("clients.html")


@app.route("/products")
def products_page():
    return render_template("products.html")


@app.route("/sales")
def sales_page():
    return render_template("sales.html")


@app.route("/reports")
def reports_page():
    return render_template("reports.html")


# ---------------------------------------------------------------------------
# API — Clients
# ---------------------------------------------------------------------------

@app.route("/api/clients", methods=["GET"])
def api_clients_list():
    q = request.args.get("q", "").strip()
    query = Client.query
    if q:
        like = f"%{q}%"
        query = query.filter(
            db.or_(
                Client.full_name.ilike(like),
                Client.phone.ilike(like),
                Client.inn.ilike(like),
            )
        )
    clients = query.order_by(Client.id.desc()).all()
    return jsonify([c.to_dict() for c in clients])


@app.route("/api/clients", methods=["POST"])
def api_clients_create():
    data = request.get_json(silent=True) or {}
    full_name = (data.get("full_name") or "").strip()
    phone = (data.get("phone") or "").strip()
    inn = (data.get("inn") or "").strip()

    errors = {}
    if not full_name:
        errors["full_name"] = "ФИО обязательно"
    if not phone:
        errors["phone"] = "Телефон обязателен"
    if errors:
        return jsonify({"errors": errors}), 400

    client = Client(full_name=full_name, phone=phone, inn=inn)
    db.session.add(client)
    db.session.commit()
    return jsonify(client.to_dict()), 201


@app.route("/api/clients/<int:client_id>", methods=["GET"])
def api_clients_get(client_id):
    client = db.session.get(Client, client_id)
    if client is None:
        return jsonify({"error": "Клиент не найден"}), 404
    return jsonify(client.to_dict())


@app.route("/api/clients/<int:client_id>", methods=["PUT"])
def api_clients_update(client_id):
    client = db.session.get(Client, client_id)
    if client is None:
        return jsonify({"error": "Клиент не найден"}), 404

    data = request.get_json(silent=True) or {}
    full_name = (data.get("full_name") or "").strip()
    phone = (data.get("phone") or "").strip()
    inn = (data.get("inn") or "").strip()

    errors = {}
    if not full_name:
        errors["full_name"] = "ФИО обязательно"
    if not phone:
        errors["phone"] = "Телефон обязателен"
    if errors:
        return jsonify({"errors": errors}), 400

    client.full_name = full_name
    client.phone = phone
    client.inn = inn
    db.session.commit()
    return jsonify(client.to_dict())


@app.route("/api/clients/<int:client_id>", methods=["DELETE"])
def api_clients_delete(client_id):
    client = db.session.get(Client, client_id)
    if client is None:
        return jsonify({"error": "Клиент не найден"}), 404
    db.session.delete(client)
    db.session.commit()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# API — Products
# ---------------------------------------------------------------------------

@app.route("/api/products", methods=["GET"])
def api_products_list():
    q = request.args.get("q", "").strip()
    query = Product.query
    if q:
        query = query.filter(Product.name.ilike(f"%{q}%"))
    products = query.order_by(Product.id.desc()).all()
    return jsonify([p.to_dict() for p in products])


@app.route("/api/products", methods=["POST"])
def api_products_create():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    try:
        price = float(data.get("price", 0))
    except (TypeError, ValueError):
        price = 0.0
    try:
        quantity = int(data.get("quantity", 0))
    except (TypeError, ValueError):
        quantity = 0

    errors = {}
    if not name:
        errors["name"] = "Название обязательно"
    if price < 0:
        errors["price"] = "Цена не может быть отрицательной"
    if quantity < 0:
        errors["quantity"] = "Количество не может быть отрицательным"
    if errors:
        return jsonify({"errors": errors}), 400

    product = Product(name=name, price=price, quantity=quantity)
    db.session.add(product)
    db.session.commit()
    return jsonify(product.to_dict()), 201


@app.route("/api/products/<int:product_id>", methods=["GET"])
def api_products_get(product_id):
    product = db.session.get(Product, product_id)
    if product is None:
        return jsonify({"error": "Товар не найден"}), 404
    return jsonify(product.to_dict())


@app.route("/api/products/<int:product_id>", methods=["PUT"])
def api_products_update(product_id):
    product = db.session.get(Product, product_id)
    if product is None:
        return jsonify({"error": "Товар не найден"}), 404

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    try:
        price = float(data.get("price", 0))
    except (TypeError, ValueError):
        price = 0.0
    try:
        quantity = int(data.get("quantity", 0))
    except (TypeError, ValueError):
        quantity = 0

    errors = {}
    if not name:
        errors["name"] = "Название обязательно"
    if price < 0:
        errors["price"] = "Цена не может быть отрицательной"
    if quantity < 0:
        errors["quantity"] = "Количество не может быть отрицательным"
    if errors:
        return jsonify({"errors": errors}), 400

    product.name = name
    product.price = price
    product.quantity = quantity
    db.session.commit()
    return jsonify(product.to_dict())


@app.route("/api/products/<int:product_id>", methods=["DELETE"])
def api_products_delete(product_id):
    product = db.session.get(Product, product_id)
    if product is None:
        return jsonify({"error": "Товар не найден"}), 404
    db.session.delete(product)
    db.session.commit()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# API — Sales
# ---------------------------------------------------------------------------

@app.route("/api/sales", methods=["GET"])
def api_sales_list():
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    client_id = request.args.get("client_id", "").strip()

    query = Sale.query
    if client_id:
        try:
            query = query.filter(Sale.client_id == int(client_id))
        except ValueError:
            return jsonify({"error": "Неверный формат client_id"}), 400
    if date_from:
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d")
            query = query.filter(Sale.date >= dt_from)
        except ValueError:
            pass
    if date_to:
        try:
            dt_to = datetime.strptime(date_to, "%Y-%m-%d")
            query = query.filter(Sale.date <= dt_to.replace(hour=23, minute=59, second=59))
        except ValueError:
            pass

    sales = query.order_by(Sale.id.desc()).all()
    return jsonify([s.to_dict() for s in sales])


@app.route("/api/sales", methods=["POST"])
def api_sales_create():
    data = request.get_json(silent=True) or {}
    try:
        client_id = int(data.get("client_id", 0))
    except (TypeError, ValueError):
        client_id = 0
    try:
        product_id = int(data.get("product_id", 0))
    except (TypeError, ValueError):
        product_id = 0
    try:
        qty = int(data.get("quantity", 1))
    except (TypeError, ValueError):
        qty = 1

    errors = {}
    if not client_id:
        errors["client_id"] = "Выберите клиента"
    if not product_id:
        errors["product_id"] = "Выберите товар"
    if qty <= 0:
        errors["quantity"] = "Количество должно быть больше 0"

    client = db.session.get(Client, client_id) if client_id else None
    product = db.session.get(Product, product_id) if product_id else None

    if client_id and not client:
        errors["client_id"] = "Клиент не найден"
    if product_id and not product:
        errors["product_id"] = "Товар не найден"
    if product and qty > product.quantity:
        errors["quantity"] = f"Недостаточно товара на складе (доступно: {product.quantity})"

    if errors:
        return jsonify({"errors": errors}), 400

    total = round(product.price * qty, 2)
    product.quantity -= qty

    date_str = (data.get("date") or "").strip()
    if date_str:
        try:
            sale_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M")
        except ValueError:
            sale_date = datetime.utcnow()
    else:
        sale_date = datetime.utcnow()

    sale = Sale(
        client_id=client_id,
        product_id=product_id,
        quantity=qty,
        total=total,
        date=sale_date,
    )
    db.session.add(sale)
    db.session.commit()
    return jsonify(sale.to_dict()), 201


@app.route("/api/sales/<int:sale_id>", methods=["GET"])
def api_sales_get(sale_id):
    sale = db.session.get(Sale, sale_id)
    if sale is None:
        return jsonify({"error": "Продажа не найдена"}), 404
    return jsonify(sale.to_dict())


@app.route("/api/sales/<int:sale_id>", methods=["DELETE"])
def api_sales_delete(sale_id):
    sale = db.session.get(Sale, sale_id)
    if sale is None:
        return jsonify({"error": "Продажа не найдена"}), 404
    # Restore stock
    product = db.session.get(Product, sale.product_id)
    if product:
        product.quantity += sale.quantity
    db.session.delete(sale)
    db.session.commit()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# API — Reports
# ---------------------------------------------------------------------------

@app.route("/api/reports", methods=["GET"])
def api_reports():
    report_type = request.args.get("type", "summary")
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()

    query = Sale.query
    if date_from:
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d")
            query = query.filter(Sale.date >= dt_from)
        except ValueError:
            pass
    if date_to:
        try:
            dt_to = datetime.strptime(date_to, "%Y-%m-%d")
            query = query.filter(Sale.date <= dt_to.replace(hour=23, minute=59, second=59))
        except ValueError:
            pass

    sales = query.all()

    if report_type == "by_client":
        data = {}
        for s in sales:
            cname = s.client.full_name if s.client else f"ID {s.client_id}"
            if cname not in data:
                data[cname] = {"client": cname, "sales_count": 0, "total": 0.0}
            data[cname]["sales_count"] += 1
            data[cname]["total"] += s.total
        result = sorted(data.values(), key=lambda x: x["total"], reverse=True)
        for r in result:
            r["total"] = round(r["total"], 2)
        return jsonify(result)

    elif report_type == "by_product":
        data = {}
        for s in sales:
            pname = s.product.name if s.product else f"ID {s.product_id}"
            if pname not in data:
                data[pname] = {"product": pname, "quantity_sold": 0, "total": 0.0}
            data[pname]["quantity_sold"] += s.quantity
            data[pname]["total"] += s.total
        result = sorted(data.values(), key=lambda x: x["total"], reverse=True)
        for r in result:
            r["total"] = round(r["total"], 2)
        return jsonify(result)

    elif report_type == "by_date":
        data = {}
        for s in sales:
            day = s.date.strftime("%d.%m.%Y") if s.date else "—"
            if day not in data:
                data[day] = {"date": day, "sales_count": 0, "total": 0.0}
            data[day]["sales_count"] += 1
            data[day]["total"] += s.total
        result = sorted(data.values(), key=lambda x: x["date"])
        for r in result:
            r["total"] = round(r["total"], 2)
        return jsonify(result)

    else:  # summary
        total_revenue = round(sum(s.total for s in sales), 2)
        total_sales = len(sales)
        total_clients = Client.query.count()
        total_products = Product.query.count()
        low_stock = Product.query.filter(Product.quantity <= 5).count()
        return jsonify({
            "total_revenue": total_revenue,
            "total_sales": total_sales,
            "total_clients": total_clients,
            "total_products": total_products,
            "low_stock": low_stock,
        })


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

@app.route("/api/export/clients")
def export_clients():
    clients = Client.query.order_by(Client.id).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "ФИО", "Телефон", "ИНН", "Дата добавления"])
    for c in clients:
        writer.writerow([
            c.id, c.full_name, c.phone, c.inn or "",
            c.created_at.strftime("%d.%m.%Y %H:%M") if c.created_at else "",
        ])
    output.seek(0)
    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=clients.csv"},
    )


@app.route("/api/export/products")
def export_products():
    products = Product.query.order_by(Product.id).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Название", "Цена", "Количество"])
    for p in products:
        writer.writerow([p.id, p.name, p.price, p.quantity])
    output.seek(0)
    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=products.csv"},
    )


@app.route("/api/export/sales")
def export_sales():
    sales = Sale.query.order_by(Sale.id).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Клиент", "Товар", "Количество", "Сумма", "Дата"])
    for s in sales:
        writer.writerow([
            s.id,
            s.client.full_name if s.client else "",
            s.product.name if s.product else "",
            s.quantity,
            s.total,
            s.date.strftime("%d.%m.%Y %H:%M") if s.date else "",
        ])
    output.seek(0)
    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=sales.csv"},
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=False)
