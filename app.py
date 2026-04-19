from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import re
from typing import Dict, List

from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import case, func, inspect, text
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)
app.config.setdefault("SQLALCHEMY_DATABASE_URI", "sqlite:///altaiglobal.db")
app.config.setdefault("SQLALCHEMY_TRACK_MODIFICATIONS", False)

db = SQLAlchemy(app)
_SAFE_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Sale(db.Model):
    __tablename__ = "sales"

    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    liters = db.Column(db.Numeric(10, 2), nullable=False, default=0)
    amount = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    payment_type = db.Column(db.String(50), nullable=True)


@app.route("/")
def index():
    return render_template("index.html")


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _to_float(value: Decimal | float | int | None) -> float:
    return float(value or 0)


def _remaining_goods_by_day(days: List[date]) -> Dict[date, float]:
    if not days:
        return {}

    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())
    table_name = "car" if "car" in table_names else "cars" if "cars" in table_names else None
    if not table_name:
        return {}

    candidate_remaining_columns = [
        "remaining_goods",
        "stock_remaining",
        "fuel_remaining",
        "liters_remaining",
        "remainder",
    ]
    columns = {column["name"] for column in inspector.get_columns(table_name)}
    remaining_column = next((name for name in candidate_remaining_columns if name in columns), None)
    if not remaining_column:
        return {}

    candidate_date_columns = ["updated_at", "created_at"]
    date_column = next((name for name in candidate_date_columns if name in columns), None)
    if not _SAFE_SQL_IDENTIFIER.match(table_name) or not _SAFE_SQL_IDENTIFIER.match(remaining_column):
        return {}
    if date_column and not _SAFE_SQL_IDENTIFIER.match(date_column):
        return {}

    if not date_column:
        total_remaining = db.session.execute(
            text(f"SELECT COALESCE(SUM({remaining_column}), 0) FROM {table_name}")
        ).scalar_one()
        return {day: float(total_remaining or 0) for day in days}

    if not days:
        return {}
    min_day = min(days).isoformat()
    max_day = max(days).isoformat()
    rows = db.session.execute(
        text(
            f"""
            SELECT DATE({date_column}) AS d, COALESCE(SUM({remaining_column}), 0) AS rem
            FROM {table_name}
            WHERE DATE({date_column}) BETWEEN :min_day AND :max_day
            GROUP BY DATE({date_column})
            """
        ),
        {"min_day": min_day, "max_day": max_day},
    ).all()
    return {datetime.strptime(row.d, "%Y-%m-%d").date(): float(row.rem or 0) for row in rows if row.d}


@app.route("/turnover")
def turnover():
    start_date = _parse_date(request.args.get("start_date"))
    end_date = _parse_date(request.args.get("end_date"))

    sale_day = func.date(Sale.created_at)
    payment_type = func.lower(func.coalesce(Sale.payment_type, ""))

    query = db.session.query(
        sale_day.label("sale_date"),
        func.coalesce(func.sum(Sale.liters), 0).label("liters"),
        func.coalesce(func.sum(Sale.amount), 0).label("amount"),
        func.coalesce(func.sum(case((payment_type == "долг", 0), else_=Sale.amount)), 0).label("payments"),
        func.coalesce(func.sum(case((payment_type == "долг", Sale.amount), else_=0)), 0).label("debts"),
    )

    if start_date:
        query = query.filter(sale_day >= start_date.isoformat())
    if end_date:
        query = query.filter(sale_day <= end_date.isoformat())

    rows_data = []
    totals = {
        "liters": 0.0,
        "amount": 0.0,
        "payments": 0.0,
        "debts": 0.0,
        "average_price": 0.0,
    }

    try:
        grouped_rows = query.group_by(sale_day).order_by(sale_day.desc()).all()
        dates = [datetime.strptime(row.sale_date, "%Y-%m-%d").date() for row in grouped_rows if row.sale_date]
        remainder_map = _remaining_goods_by_day(dates)

        for row in grouped_rows:
            if not row.sale_date:
                continue
            row_date = datetime.strptime(row.sale_date, "%Y-%m-%d").date()
            liters = _to_float(row.liters)
            amount = _to_float(row.amount)
            payments = _to_float(row.payments)
            debts = _to_float(row.debts)
            average_price = amount / liters if liters else 0.0

            totals["liters"] += liters
            totals["amount"] += amount
            totals["payments"] += payments
            totals["debts"] += debts

            rows_data.append(
                {
                    "date": row_date,
                    "liters": liters,
                    "amount": amount,
                    "payments": payments,
                    "debts": debts,
                    "average_price": average_price,
                    "remaining_goods": remainder_map.get(row_date),
                }
            )

        totals["average_price"] = totals["amount"] / totals["liters"] if totals["liters"] else 0.0
    except SQLAlchemyError:
        app.logger.exception(
            "Failed to build turnover report for date range %s - %s",
            start_date.isoformat() if start_date else "any",
            end_date.isoformat() if end_date else "any",
        )
        rows_data = []

    return render_template(
        "turnover.html",
        rows=rows_data,
        totals=totals,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
    )


if __name__ == "__main__":
    app.run()
