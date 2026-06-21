"""
Sample SQLite database for exercising / demoing the RAG pipeline.

``build_sample_db(path)`` creates a small but realistic e-commerce schema with
seed data so the index/retrieve/ask flow can be tested without a real server.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

_SCHEMA = """
CREATE TABLE customers (
    customer_id   INTEGER PRIMARY KEY,
    full_name     TEXT NOT NULL,
    email         TEXT NOT NULL UNIQUE,
    country       TEXT,
    signup_date   TEXT,
    loyalty_tier  TEXT DEFAULT 'bronze'
);

CREATE TABLE categories (
    category_id   INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT
);

CREATE TABLE products (
    product_id    INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    category_id   INTEGER REFERENCES categories(category_id),
    unit_price    REAL NOT NULL,
    in_stock      INTEGER NOT NULL DEFAULT 0,
    status        TEXT DEFAULT 'active'
);

CREATE TABLE orders (
    order_id      INTEGER PRIMARY KEY,
    customer_id   INTEGER REFERENCES customers(customer_id),
    order_date    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    total_amount  REAL NOT NULL DEFAULT 0
);

CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id      INTEGER REFERENCES orders(order_id),
    product_id    INTEGER REFERENCES products(product_id),
    quantity      INTEGER NOT NULL,
    line_total    REAL NOT NULL
);

CREATE TABLE payments (
    payment_id    INTEGER PRIMARY KEY,
    order_id      INTEGER REFERENCES orders(order_id),
    method        TEXT NOT NULL,
    amount        REAL NOT NULL,
    paid_at       TEXT
);

CREATE INDEX ix_orders_customer ON orders(customer_id);
CREATE INDEX ix_order_items_order ON order_items(order_id);

CREATE VIEW v_customer_spend AS
    SELECT c.customer_id, c.full_name, SUM(o.total_amount) AS lifetime_spend
    FROM customers c
    LEFT JOIN orders o ON o.customer_id = c.customer_id
    GROUP BY c.customer_id, c.full_name;
"""

_CUSTOMERS = [
    (1, "Alice Martin", "alice@example.com", "USA", "2024-01-15", "gold"),
    (2, "Bob Verma", "bob@example.com", "India", "2024-03-02", "silver"),
    (3, "Chloe Dupont", "chloe@example.com", "France", "2024-05-21", "bronze"),
    (4, "Daniel Kim", "daniel@example.com", "South Korea", "2024-06-10", "gold"),
    (5, "Eva Rossi", "eva@example.com", "Italy", "2024-07-19", "silver"),
]

_CATEGORIES = [
    (1, "Electronics", "Phones, laptops and accessories"),
    (2, "Books", "Printed and digital books"),
    (3, "Home", "Home and kitchen goods"),
]

_PRODUCTS = [
    (1, "Smartphone X", 1, 699.00, 25, "active"),
    (2, "Laptop Pro", 1, 1299.00, 10, "active"),
    (3, "USB-C Cable", 1, 12.50, 200, "active"),
    (4, "SQL Cookbook", 2, 39.99, 50, "active"),
    (5, "Coffee Maker", 3, 89.00, 0, "discontinued"),
    (6, "Desk Lamp", 3, 24.00, 75, "active"),
]

_ORDERS = [
    (1, 1, "2024-08-01", "shipped", 711.50),
    (2, 2, "2024-08-03", "pending", 39.99),
    (3, 1, "2024-08-10", "delivered", 1299.00),
    (4, 4, "2024-08-12", "cancelled", 24.00),
    (5, 5, "2024-08-15", "shipped", 113.00),
]

_ORDER_ITEMS = [
    (1, 1, 1, 1, 699.00),
    (2, 1, 3, 1, 12.50),
    (3, 2, 4, 1, 39.99),
    (4, 3, 2, 1, 1299.00),
    (5, 4, 6, 1, 24.00),
    (6, 5, 3, 2, 25.00),
    (7, 5, 6, 1, 24.00),
    (8, 5, 5, 1, 89.00),
]

_PAYMENTS = [
    (1, 1, "credit_card", 711.50, "2024-08-01"),
    (2, 3, "paypal", 1299.00, "2024-08-10"),
    (3, 5, "credit_card", 113.00, "2024-08-15"),
]


def build_sample_db(path: str | Path) -> str:
    """Create (overwrite) a sample SQLite DB at *path*; return its path."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        os.remove(p)

    conn = sqlite3.connect(str(p))
    try:
        conn.executescript(_SCHEMA)
        conn.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS)
        conn.executemany("INSERT INTO categories VALUES (?,?,?)", _CATEGORIES)
        conn.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", _PRODUCTS)
        conn.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS)
        conn.executemany("INSERT INTO order_items VALUES (?,?,?,?,?)", _ORDER_ITEMS)
        conn.executemany("INSERT INTO payments VALUES (?,?,?,?,?)", _PAYMENTS)
        conn.commit()
    finally:
        conn.close()
    return str(p)


def build_sample_manager(path: str | Path):
    """Build the sample DB and return a connected :class:`DatabaseManager`."""
    from common.db_manager import DatabaseManager

    db_path = build_sample_db(path)
    mgr = DatabaseManager("SQLite")
    mgr.connect(database=db_path)
    return mgr
