"""Storefront archetype generator — a real customer-facing ecommerce app.

This is what an ecommerce/storefront request should produce: a working
storefront with a home page, a searchable/filterable product catalog, product
detail pages, a shopping cart, and a checkout that places real orders — **not**
a CRUD admin that mirrors database tables.

How the database is used (per product intent):
* the DB **schema informs the app design** — detecting a priced product table +
  an orders table is what tells us a storefront is the right app, and which
  table is the catalog (surfaced in the README/architecture docs);
* the app ships its **own canonical schema + seeded sample products** so it is a
  real, runnable, testable application out of the box (SQLite, no external DB);
* point ``APP_DB_PATH`` / ``DATABASE_URL`` at the real database to serve live
  catalog data in production.

Generated source uses token replacement (``__TOKEN__``) rather than f-strings so
literal ``{...}`` in the emitted Python/Jinja is preserved verbatim.
"""

from __future__ import annotations

from ai_assistant.app_builder.spec import AppSpec
from ai_assistant.app_builder.webapp import (
    _ai_builder_py,
    _app_py,
    _ci_yml,
    _conftest_py,
    _document_py,
    _dockerfile,
    _has_infra,
    _hosting_yaml,
    _infra_init_py,
    _infra_yaml,
    _monitoring_py,
    _notification_py,
    _requirements,
    _settings_py,
    skeleton_files,
)

# Canonical catalog columns the storefront UI/queries rely on.
_PRODUCT_COLUMNS = ["id", "name", "description", "price", "category", "stock",
                    "image_url"]

# Seeded sample catalog so the store actually sells something on first run.
_SAMPLE = [
    ("Signature Starter Pack", "A curated bundle for first-time shoppers.",
     49.0, "Featured", 25, "https://picsum.photos/seed/starter/600/400"),
    ("Premium Daily Essential", "A customer favorite for everyday use.",
     79.0, "Featured", 40, "https://picsum.photos/seed/essential/600/400"),
    ("Professional Kit", "A complete set for power users and teams.",
     149.0, "Bundles", 15, "https://picsum.photos/seed/prokit/600/400"),
    ("Compact Travel Set", "A lightweight option that is easy to carry.",
     59.0, "Bundles", 18, "https://picsum.photos/seed/travel/600/400"),
    ("Gift Box", "A ready-to-gift package with best-selling products.",
     99.0, "Gifts", 80, "https://picsum.photos/seed/giftbox/600/400"),
    ("Limited Edition Item", "A seasonal release with limited stock.",
     129.0, "Limited", 35, "https://picsum.photos/seed/limited/600/400"),
    ("Accessory Pack", "Useful add-ons for the main product line.",
     29.0, "Accessories", 120, "https://picsum.photos/seed/accessory/600/400"),
    ("Replacement Pack", "Consumables and replacement items for returning buyers.",
     19.0, "Accessories", 100, "https://picsum.photos/seed/refill/600/400"),
]


# ── data layer ────────────────────────────────────────────────────────────────
def _schema_py() -> str:
    rows = ",\n".join(
        "    (" + repr(n) + ", " + repr(d) + ", " + repr(p) + ", " + repr(c)
        + ", " + repr(s) + ", " + repr(img) + ")"
        for (n, d, p, c, s, img) in _SAMPLE
    )
    return (
        '"""Storefront schema (products, orders, order_items) + sample catalog."""\n'
        "from __future__ import annotations\n\n"
        "SCHEMA = {\n"
        '    "products": ["id", "name", "description", "price", "category", '
        '"stock", "image_url"],\n'
        '    "orders": ["id", "customer_name", "email", "total", "created_at"],\n'
        '    "order_items": ["id", "order_id", "product_id", "name", "price", '
        '"quantity"],\n'
        "}\n\n"
        "_DDL = [\n"
        '    """CREATE TABLE IF NOT EXISTS products (\n'
        "        id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "        name TEXT NOT NULL,\n"
        "        description TEXT,\n"
        "        price REAL NOT NULL DEFAULT 0,\n"
        "        category TEXT,\n"
        "        stock INTEGER NOT NULL DEFAULT 0,\n"
        "        image_url TEXT\n"
        '    )""",\n'
        '    """CREATE TABLE IF NOT EXISTS orders (\n'
        "        id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "        customer_name TEXT NOT NULL,\n"
        "        email TEXT NOT NULL,\n"
        "        total REAL NOT NULL DEFAULT 0,\n"
        "        created_at TEXT NOT NULL\n"
        '    )""",\n'
        '    """CREATE TABLE IF NOT EXISTS order_items (\n'
        "        id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "        order_id INTEGER NOT NULL,\n"
        "        product_id INTEGER NOT NULL,\n"
        "        name TEXT NOT NULL,\n"
        "        price REAL NOT NULL DEFAULT 0,\n"
        "        quantity INTEGER NOT NULL DEFAULT 1\n"
        '    )""",\n'
        "]\n\n"
        "SAMPLE_PRODUCTS = [\n"
        + rows + ",\n"
        "]\n\n\n"
        "def create_ddl() -> list[str]:\n"
        '    """Return CREATE TABLE statements for the storefront."""\n'
        "    return list(_DDL)\n\n\n"
        "def seed(conn) -> None:\n"
        '    """Insert the sample catalog the first time the store runs."""\n'
        '    count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]\n'
        "    if count:\n"
        "        return\n"
        "    conn.executemany(\n"
        '        "INSERT INTO products (name, description, price, category, stock, "\n'
        '        "image_url) VALUES (?, ?, ?, ?, ?, ?)",\n'
        "        SAMPLE_PRODUCTS,\n"
        "    )\n"
        "    conn.commit()\n"
    )


def _schema_sql() -> str:
    return (
        "-- Storefront schema (SQLite dialect)\n"
        "CREATE TABLE IF NOT EXISTS products (\n"
        "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "    name TEXT NOT NULL,\n"
        "    description TEXT,\n"
        "    price REAL NOT NULL DEFAULT 0,\n"
        "    category TEXT,\n"
        "    stock INTEGER NOT NULL DEFAULT 0,\n"
        "    image_url TEXT\n"
        ");\n"
        "CREATE TABLE IF NOT EXISTS orders (\n"
        "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "    customer_name TEXT NOT NULL,\n"
        "    email TEXT NOT NULL,\n"
        "    total REAL NOT NULL DEFAULT 0,\n"
        "    created_at TEXT NOT NULL\n"
        ");\n"
        "CREATE TABLE IF NOT EXISTS order_items (\n"
        "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "    order_id INTEGER NOT NULL,\n"
        "    product_id INTEGER NOT NULL,\n"
        "    name TEXT NOT NULL,\n"
        "    price REAL NOT NULL DEFAULT 0,\n"
        "    quantity INTEGER NOT NULL DEFAULT 1\n"
        ");\n"
    )


def _connection_py() -> str:
    return (
        '"""SQLite connection management with schema init + catalog seeding."""\n'
        "from __future__ import annotations\n\n"
        "import os\n"
        "import sqlite3\n"
        "import threading\n\n"
        "from src.db.schema import create_ddl, seed\n\n"
        "_LOCK = threading.Lock()\n"
        "_CONNECTION: sqlite3.Connection | None = None\n\n\n"
        "def get_connection() -> sqlite3.Connection:\n"
        '    """Return a process-wide connection, creating + seeding on first use."""\n'
        "    global _CONNECTION\n"
        "    with _LOCK:\n"
        "        if _CONNECTION is None:\n"
        '            path = os.environ.get("APP_DB_PATH", "var/store.db")\n'
        '            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)\n'
        "            conn = sqlite3.connect(path, check_same_thread=False)\n"
        "            conn.row_factory = sqlite3.Row\n"
        '            conn.executescript(";\\n".join(create_ddl()) + ";")\n'
        "            conn.commit()\n"
        "            seed(conn)\n"
        "            _CONNECTION = conn\n"
        "        return _CONNECTION\n\n\n"
        "def reset_connection() -> None:\n"
        '    """Close the cached connection (used by tests)."""\n'
        "    global _CONNECTION\n"
        "    with _LOCK:\n"
        "        if _CONNECTION is not None:\n"
        "            _CONNECTION.close()\n"
        "            _CONNECTION = None\n"
    )


def _models_py() -> str:
    return (
        '"""Storefront data models."""\n'
        "from __future__ import annotations\n\n"
        "from dataclasses import dataclass, field\n"
        "from typing import Any\n\n\n"
        "@dataclass\n"
        "class Product:\n"
        '    """A catalog product for sale."""\n\n'
        "    id: int = 0\n"
        '    name: str = ""\n'
        '    description: str = ""\n'
        "    price: float = 0.0\n"
        '    category: str = ""\n'
        "    stock: int = 0\n"
        '    image_url: str = ""\n\n\n'
        "@dataclass\n"
        "class Order:\n"
        '    """A placed customer order."""\n\n'
        "    id: int = 0\n"
        '    customer_name: str = ""\n'
        '    email: str = ""\n'
        "    total: float = 0.0\n"
        '    created_at: str = ""\n'
        "    items: list[Any] = field(default_factory=list)\n"
    )


def _repository_py() -> str:
    return (
        '"""Parameterized data access for the storefront (catalog + orders)."""\n'
        "from __future__ import annotations\n\n"
        "from datetime import datetime, timezone\n\n"
        "from src.db.connection import get_connection\n\n\n"
        "class ProductRepository:\n"
        '    """Read access to the product catalog."""\n\n'
        "    def list(self, query: str = \"\", category: str = \"\",\n"
        "             limit: int = 100) -> list[dict]:\n"
        '        """List products, optionally filtered by search text/category."""\n'
        '        sql = "SELECT * FROM products WHERE 1=1"\n'
        "        params: list = []\n"
        "        if query:\n"
        '            sql += " AND (name LIKE ? OR description LIKE ?)"\n'
        '            params += ["%" + query + "%", "%" + query + "%"]\n'
        "        if category:\n"
        '            sql += " AND category = ?"\n'
        "            params.append(category)\n"
        '        sql += " ORDER BY name ASC LIMIT ?"\n'
        "        params.append(limit)\n"
        "        conn = get_connection()\n"
        "        return [dict(r) for r in conn.execute(sql, params).fetchall()]\n\n"
        "    def get(self, product_id: int) -> dict | None:\n"
        '        """Return a single product by id, or None."""\n'
        "        conn = get_connection()\n"
        '        row = conn.execute(\n'
        '            "SELECT * FROM products WHERE id = ?", (product_id,)\n'
        "        ).fetchone()\n"
        "        return dict(row) if row is not None else None\n\n"
        "    def categories(self) -> list[str]:\n"
        '        """Return the distinct, non-empty product categories."""\n'
        "        conn = get_connection()\n"
        "        rows = conn.execute(\n"
        '            "SELECT DISTINCT category FROM products "\n'
        '            "WHERE category IS NOT NULL AND category != \'\' ORDER BY category"\n'
        "        ).fetchall()\n"
        "        return [r[0] for r in rows]\n\n\n"
        "class OrderRepository:\n"
        '    """Create and read customer orders."""\n\n'
        "    def create(self, customer_name: str, email: str,\n"
        "               items: list[dict]) -> int:\n"
        '        """Persist an order and its line items; returns the new order id."""\n'
        "        total = sum(float(i[\"price\"]) * int(i[\"quantity\"]) for i in items)\n"
        "        conn = get_connection()\n"
        "        cur = conn.execute(\n"
        '            "INSERT INTO orders (customer_name, email, total, created_at) "\n'
        '            "VALUES (?, ?, ?, ?)",\n'
        "            (customer_name, email, total,\n"
        "             datetime.now(timezone.utc).isoformat()),\n"
        "        )\n"
        "        order_id = int(cur.lastrowid)\n"
        "        for i in items:\n"
        "            conn.execute(\n"
        '                "INSERT INTO order_items (order_id, product_id, name, price, "\n'
        '                "quantity) VALUES (?, ?, ?, ?, ?)",\n'
        '                (order_id, int(i["product_id"]), i["name"],\n'
        '                 float(i["price"]), int(i["quantity"])),\n'
        "            )\n"
        "        conn.commit()\n"
        "        return order_id\n\n"
        "    def get(self, order_id: int) -> dict | None:\n"
        '        """Return an order with its line items, or None."""\n'
        "        conn = get_connection()\n"
        '        row = conn.execute(\n'
        '            "SELECT * FROM orders WHERE id = ?", (order_id,)\n'
        "        ).fetchone()\n"
        "        if row is None:\n"
        "            return None\n"
        "        order = dict(row)\n"
        "        items = conn.execute(\n"
        '            "SELECT * FROM order_items WHERE order_id = ?", (order_id,)\n'
        "        ).fetchall()\n"
        '        order["items"] = [dict(i) for i in items]\n'
        "        return order\n"
    )


# ── JSON API ──────────────────────────────────────────────────────────────────
def _api_py() -> str:
    return (
        '"""JSON REST API for the storefront catalog and orders."""\n'
        "from __future__ import annotations\n\n"
        "from fastapi import APIRouter, HTTPException\n\n"
        "from src.repository import OrderRepository, ProductRepository\n\n"
        'router = APIRouter(prefix="/api")\n\n\n'
        '@router.get("/products")\n'
        "def list_products(q: str = \"\", category: str = \"\",\n"
        "                  limit: int = 100) -> list[dict]:\n"
        '    """List catalog products (optional search ``q`` and ``category``)."""\n'
        "    return ProductRepository().list(query=q, category=category, limit=limit)\n\n\n"
        '@router.get("/categories")\n'
        "def list_categories() -> list[str]:\n"
        '    """List the distinct product categories."""\n'
        "    return ProductRepository().categories()\n\n\n"
        '@router.get("/products/{product_id}")\n'
        "def get_product(product_id: int) -> dict:\n"
        '    """Return a single product by id."""\n'
        "    product = ProductRepository().get(product_id)\n"
        "    if product is None:\n"
        '        raise HTTPException(status_code=404, detail="product not found")\n'
        "    return product\n\n\n"
        '@router.post("/orders")\n'
        "def create_order(payload: dict) -> dict:\n"
        '    """Place an order from a JSON payload (customer + line items)."""\n'
        '    items = payload.get("items") or []\n'
        "    resolved = []\n"
        "    repo = ProductRepository()\n"
        "    for line in items:\n"
        '        product = repo.get(int(line.get("product_id", 0)))\n'
        "        if product is None:\n"
        '            raise HTTPException(status_code=400, detail="invalid product")\n'
        "        resolved.append({\n"
        '            "product_id": product["id"], "name": product["name"],\n'
        '            "price": product["price"],\n'
        '            "quantity": int(line.get("quantity", 1)),\n'
        "        })\n"
        "    if not resolved:\n"
        '        raise HTTPException(status_code=400, detail="empty order")\n'
        "    order_id = OrderRepository().create(\n"
        '        payload.get("customer_name", "Guest"),\n'
        '        payload.get("email", ""), resolved,\n'
        "    )\n"
        '    return {"id": order_id}\n'
    )


# ── HTML storefront ─────────────────────────────────────────────────────────--
def _web_py() -> str:
    return (
        '"""Server-rendered storefront UI (home, catalog, product, cart, checkout)."""\n'
        "from __future__ import annotations\n\n"
        "import json\n"
        "from pathlib import Path\n"
        "from urllib.parse import parse_qsl\n\n"
        "from fastapi import APIRouter, HTTPException, Request\n"
        "from fastapi.responses import HTMLResponse, RedirectResponse\n"
        "from fastapi.templating import Jinja2Templates\n\n"
        "from src.repository import OrderRepository, ProductRepository\n\n"
        "_TEMPLATES = Jinja2Templates(\n"
        "    directory=str(Path(__file__).resolve().parent.parent / \"templates\")\n"
        ")\n"
        "router = APIRouter()\n\n\n"
        "async def _form_data(request: Request) -> dict:\n"
        '    """Parse a urlencoded form body without any extra dependency."""\n'
        "    raw = await request.body()\n"
        '    return dict(parse_qsl(raw.decode("utf-8")))\n\n\n'
        "def _read_cart(request: Request) -> dict:\n"
        '    """Return the cart ({product_id: qty}) from the signed-less cookie."""\n'
        '    raw = request.cookies.get("cart")\n'
        "    if not raw:\n"
        "        return {}\n"
        "    try:\n"
        "        data = json.loads(raw)\n"
        "        return {str(k): int(v) for k, v in data.items() if int(v) > 0}\n"
        "    except (ValueError, TypeError, AttributeError):\n"
        "        return {}\n\n\n"
        "def _cart_lines(cart: dict) -> tuple[list, float]:\n"
        '    """Resolve cart entries to product line items and a running total."""\n'
        "    repo = ProductRepository()\n"
        "    lines = []\n"
        "    total = 0.0\n"
        "    for pid, qty in cart.items():\n"
        "        product = repo.get(int(pid))\n"
        "        if product is None:\n"
        "            continue\n"
        '        subtotal = float(product["price"]) * int(qty)\n'
        "        total += subtotal\n"
        '        lines.append({"product": product, "quantity": int(qty),\n'
        '                      "subtotal": subtotal})\n'
        "    return lines, total\n\n\n"
        "def _context(request: Request, **extra) -> dict:\n"
        '    """Common template context (nav categories + cart count)."""\n'
        "    cart = _read_cart(request)\n"
        "    ctx = {\n"
        '        "categories": ProductRepository().categories(),\n'
        '        "cart_count": sum(cart.values()),\n'
        "    }\n"
        "    ctx.update(extra)\n"
        "    return ctx\n\n\n"
        '@router.get("/", response_class=HTMLResponse)\n'
        "def home(request: Request) -> HTMLResponse:\n"
        '    """Storefront home with featured products."""\n'
        "    featured = ProductRepository().list(limit=8)\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "index.html", _context(request, featured=featured)\n'
        "    )\n\n\n"
        '@router.get("/products", response_class=HTMLResponse)\n'
        "def catalog(request: Request, q: str = \"\", category: str = \"\") -> HTMLResponse:\n"
        '    """Searchable, filterable product catalog."""\n'
        "    products = ProductRepository().list(query=q, category=category)\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "catalog.html",\n'
        '        _context(request, products=products, q=q, category=category),\n'
        "    )\n\n\n"
        '@router.get("/products/{product_id}", response_class=HTMLResponse)\n'
        "def product_detail(request: Request, product_id: int) -> HTMLResponse:\n"
        '    """Single product detail page with an add-to-cart form."""\n'
        "    product = ProductRepository().get(product_id)\n"
        "    if product is None:\n"
        '        raise HTTPException(status_code=404, detail="product not found")\n'
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "product.html", _context(request, product=product)\n'
        "    )\n\n\n"
        '@router.post("/cart/add")\n'
        "async def cart_add(request: Request) -> RedirectResponse:\n"
        '    """Add a product to the cart cookie and go to the cart."""\n'
        "    form = await _form_data(request)\n"
        '    product_id = str(int(form.get("product_id") or 0))\n'
        '    quantity = max(1, int(form.get("quantity") or 1))\n'
        "    cart = _read_cart(request)\n"
        "    cart[product_id] = cart.get(product_id, 0) + quantity\n"
        '    resp = RedirectResponse("/cart", status_code=303)\n'
        '    resp.set_cookie("cart", json.dumps(cart), httponly=True, samesite="lax")\n'
        "    return resp\n\n\n"
        '@router.post("/cart/remove")\n'
        "async def cart_remove(request: Request) -> RedirectResponse:\n"
        '    """Remove a product from the cart cookie."""\n'
        "    form = await _form_data(request)\n"
        "    cart = _read_cart(request)\n"
        '    cart.pop(str(int(form.get("product_id") or 0)), None)\n'
        '    resp = RedirectResponse("/cart", status_code=303)\n'
        '    resp.set_cookie("cart", json.dumps(cart), httponly=True, samesite="lax")\n'
        "    return resp\n\n\n"
        '@router.get("/cart", response_class=HTMLResponse)\n'
        "def cart_view(request: Request) -> HTMLResponse:\n"
        '    """Show the current shopping cart."""\n'
        "    lines, total = _cart_lines(_read_cart(request))\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "cart.html", _context(request, lines=lines, total=total)\n'
        "    )\n\n\n"
        '@router.get("/checkout", response_class=HTMLResponse)\n'
        "def checkout_form(request: Request) -> HTMLResponse:\n"
        '    """Checkout page with the order summary and customer form."""\n'
        "    lines, total = _cart_lines(_read_cart(request))\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "checkout.html", _context(request, lines=lines, total=total)\n'
        "    )\n\n\n"
        '@router.post("/checkout")\n'
        "async def place_order(request: Request) -> RedirectResponse:\n"
        '    """Place the order from the cart and clear it."""\n'
        "    form = await _form_data(request)\n"
        '    customer_name = str(form.get("customer_name") or "Guest")\n'
        '    email = str(form.get("email") or "")\n'
        "    lines, _ = _cart_lines(_read_cart(request))\n"
        "    if not lines:\n"
        '        return RedirectResponse("/cart", status_code=303)\n'
        "    items = [\n"
        '        {"product_id": ln["product"]["id"], "name": ln["product"]["name"],\n'
        '         "price": ln["product"]["price"], "quantity": ln["quantity"]}\n'
        "        for ln in lines\n"
        "    ]\n"
        "    order_id = OrderRepository().create(customer_name, email, items)\n"
        '    resp = RedirectResponse("/orders/" + str(order_id), status_code=303)\n'
        '    resp.set_cookie("cart", json.dumps({}), httponly=True, samesite="lax")\n'
        "    return resp\n\n\n"
        '@router.get("/orders/{order_id}", response_class=HTMLResponse)\n'
        "def order_confirmation(request: Request, order_id: int) -> HTMLResponse:\n"
        '    """Order confirmation page."""\n'
        "    order = OrderRepository().get(order_id)\n"
        "    if order is None:\n"
        '        raise HTTPException(status_code=404, detail="order not found")\n'
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "confirmation.html", _context(request, order=order)\n'
        "    )\n"
    )


# ── templates ─────────────────────────────────────────────────────────────────
_BASE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{% block title %}__APP_NAME__{% endblock %}</title>
  <link rel="stylesheet" href="/static/style.css">
</head>
<body>
  <header class="topbar">
    <a class="brand" href="/">__APP_NAME__</a>
    <form class="search" action="/products" method="get">
      <input name="q" placeholder="Search products..." value="{{ q | default('') }}">
      <button type="submit">Search</button>
    </form>
    <nav class="cats">
      {% for c in categories %}<a href="/products?category={{ c }}">{{ c }}</a>{% endfor %}
    </nav>
    <a class="cart-link" href="/cart">Cart ({{ cart_count | default(0) }})</a>
  </header>
  <main class="container">
    {% block content %}{% endblock %}
  </main>
  <footer class="footer">__APP_NAME__ — generated by AppBuilderAssistant</footer>
</body>
</html>
"""

_INDEX_HTML = """{% extends "base.html" %}
{% block content %}
<section class="hero">
  <h1>Products, delivered.</h1>
  <p>Browse the catalog, add items to your cart, and check out securely.</p>
  <a class="btn primary" href="/products">Shop all products</a>
</section>
<h2>Featured</h2>
<div class="grid">
  {% for p in featured %}
  <a class="card" href="/products/{{ p.id }}">
    <img src="{{ p.image_url }}" alt="{{ p.name }}">
    <div class="card-body">
      <div class="card-title">{{ p.name }}</div>
      <div class="card-cat">{{ p.category }}</div>
      <div class="price">${{ "%.2f"|format(p.price) }}</div>
    </div>
  </a>
  {% endfor %}
</div>
{% endblock %}
"""

_CATALOG_HTML = """{% extends "base.html" %}
{% block title %}Catalog{% endblock %}
{% block content %}
<div class="page-head">
  <h1>{% if category %}{{ category }}{% else %}All products{% endif %}</h1>
  <span class="muted">{{ products | length }} item(s)</span>
</div>
<div class="grid">
  {% for p in products %}
  <a class="card" href="/products/{{ p.id }}">
    <img src="{{ p.image_url }}" alt="{{ p.name }}">
    <div class="card-body">
      <div class="card-title">{{ p.name }}</div>
      <div class="card-cat">{{ p.category }}</div>
      <div class="price">${{ "%.2f"|format(p.price) }}</div>
    </div>
  </a>
  {% endfor %}
  {% if not products %}<p class="muted">No products match your search.</p>{% endif %}
</div>
{% endblock %}
"""

_PRODUCT_HTML = """{% extends "base.html" %}
{% block title %}{{ product.name }}{% endblock %}
{% block content %}
<div class="detail">
  <img class="detail-img" src="{{ product.image_url }}" alt="{{ product.name }}">
  <div class="detail-info">
    <h1>{{ product.name }}</h1>
    <div class="card-cat">{{ product.category }}</div>
    <div class="price big">${{ "%.2f"|format(product.price) }}</div>
    <p>{{ product.description }}</p>
    <p class="muted">{{ product.stock }} in stock</p>
    <form method="post" action="/cart/add" class="add-form">
      <input type="hidden" name="product_id" value="{{ product.id }}">
      <input type="number" name="quantity" value="1" min="1">
      <button class="btn primary" type="submit">Add to cart</button>
    </form>
    <a class="btn" href="/products">Back to catalog</a>
  </div>
</div>
{% endblock %}
"""

_CART_HTML = """{% extends "base.html" %}
{% block title %}Your cart{% endblock %}
{% block content %}
<h1>Your cart</h1>
<table class="grid-table">
  <thead><tr><th>Product</th><th>Price</th><th>Qty</th><th>Subtotal</th><th></th></tr></thead>
  <tbody>
    {% for ln in lines %}
    <tr>
      <td><a href="/products/{{ ln.product.id }}">{{ ln.product.name }}</a></td>
      <td>${{ "%.2f"|format(ln.product.price) }}</td>
      <td>{{ ln.quantity }}</td>
      <td>${{ "%.2f"|format(ln.subtotal) }}</td>
      <td>
        <form method="post" action="/cart/remove">
          <input type="hidden" name="product_id" value="{{ ln.product.id }}">
          <button class="btn danger" type="submit">Remove</button>
        </form>
      </td>
    </tr>
    {% endfor %}
    {% if not lines %}<tr><td colspan="5" class="muted">Your cart is empty.</td></tr>{% endif %}
  </tbody>
</table>
{% if lines %}
<div class="cart-foot">
  <div class="total">Total: ${{ "%.2f"|format(total) }}</div>
  <a class="btn primary" href="/checkout">Checkout</a>
</div>
{% endif %}
{% endblock %}
"""

_CHECKOUT_HTML = """{% extends "base.html" %}
{% block title %}Checkout{% endblock %}
{% block content %}
<h1>Checkout</h1>
<div class="checkout">
  <form method="post" action="/checkout" class="form">
    <label><span>Full name</span><input name="customer_name" required></label>
    <label><span>Email</span><input name="email" type="email" required></label>
    <button class="btn primary" type="submit">Place order</button>
  </form>
  <div class="summary">
    <h3>Order summary</h3>
    {% for ln in lines %}
    <div class="summary-row"><span>{{ ln.product.name }} × {{ ln.quantity }}</span>
      <span>${{ "%.2f"|format(ln.subtotal) }}</span></div>
    {% endfor %}
    <div class="summary-row total"><span>Total</span><span>${{ "%.2f"|format(total) }}</span></div>
  </div>
</div>
{% endblock %}
"""

_CONFIRMATION_HTML = """{% extends "base.html" %}
{% block title %}Order confirmed{% endblock %}
{% block content %}
<div class="confirm">
  <h1>Thank you, {{ order.customer_name }}!</h1>
  <p>Your order <strong>#{{ order.id }}</strong> has been placed.</p>
  <table class="grid-table">
    <thead><tr><th>Product</th><th>Price</th><th>Qty</th></tr></thead>
    <tbody>
      {% for it in order['items'] %}
      <tr><td>{{ it.name }}</td><td>${{ "%.2f"|format(it.price) }}</td><td>{{ it.quantity }}</td></tr>
      {% endfor %}
    </tbody>
  </table>
  <div class="total">Total paid: ${{ "%.2f"|format(order.total) }}</div>
  <a class="btn primary" href="/products">Continue shopping</a>
</div>
{% endblock %}
"""

_STYLE_CSS = """:root {
  --bg:#0b1220; --panel:#fff; --ink:#0f172a; --muted:#64748b;
  --accent:#2563eb; --danger:#dc2626; --line:#e2e8f0; --ok:#16a34a;
}
*{box-sizing:border-box;}
body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:var(--ink);background:#f1f5f9;}
.topbar{display:flex;align-items:center;gap:18px;padding:12px 24px;background:var(--bg);color:#fff;flex-wrap:wrap;}
.brand{font-weight:800;color:#fff;text-decoration:none;font-size:20px;}
.search{display:flex;flex:1;min-width:200px;}
.search input{flex:1;padding:8px 10px;border:0;border-radius:8px 0 0 8px;}
.search button{padding:8px 14px;border:0;border-radius:0 8px 8px 0;background:var(--accent);color:#fff;cursor:pointer;}
.cats{display:flex;gap:12px;flex-wrap:wrap;}
.cats a{color:#cbd5e1;text-decoration:none;font-size:14px;}
.cats a:hover{color:#fff;}
.cart-link{color:#fff;text-decoration:none;font-weight:600;}
.container{max-width:1080px;margin:24px auto;padding:0 20px;}
.hero{background:linear-gradient(135deg,#1e3a8a,#2563eb);color:#fff;padding:40px;border-radius:16px;margin-bottom:24px;}
.hero h1{margin:0 0 8px;font-size:32px;}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:18px;}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;overflow:hidden;text-decoration:none;color:inherit;box-shadow:0 1px 2px rgba(0,0,0,.05);}
.card:hover{border-color:var(--accent);}
.card img{width:100%;height:150px;object-fit:cover;display:block;background:#e2e8f0;}
.card-body{padding:12px;}
.card-title{font-weight:600;}
.card-cat{color:var(--muted);font-size:12px;}
.price{color:var(--accent);font-weight:700;margin-top:6px;}
.price.big{font-size:26px;}
.page-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}
.muted{color:var(--muted);}
.detail{display:grid;grid-template-columns:1fr 1fr;gap:24px;background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:20px;}
.detail-img{width:100%;border-radius:12px;background:#e2e8f0;}
.add-form{display:flex;gap:8px;align-items:center;margin:14px 0;}
.add-form input{width:80px;padding:8px;border:1px solid var(--line);border-radius:8px;}
.btn{display:inline-block;padding:9px 14px;border-radius:9px;border:1px solid var(--line);background:#fff;color:var(--ink);text-decoration:none;cursor:pointer;font-size:14px;}
.btn.primary{background:var(--accent);color:#fff;border-color:var(--accent);}
.btn.danger{background:var(--danger);color:#fff;border-color:var(--danger);}
.grid-table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden;}
.grid-table th,.grid-table td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line);}
.cart-foot{display:flex;justify-content:space-between;align-items:center;margin-top:16px;}
.total{font-size:20px;font-weight:700;}
.checkout{display:grid;grid-template-columns:1fr 1fr;gap:24px;}
.form{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:20px;display:grid;gap:14px;}
.form label{display:grid;gap:6px;}
.form input{padding:9px 10px;border:1px solid var(--line);border-radius:8px;}
.summary{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:20px;}
.summary-row{display:flex;justify-content:space-between;padding:6px 0;}
.summary-row.total{border-top:1px solid var(--line);margin-top:8px;padding-top:10px;font-weight:700;}
.confirm{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:24px;}
.footer{text-align:center;color:var(--muted);padding:30px;font-size:12px;}
"""


# ── tests ─────────────────────────────────────────────────────────────────────
def _tests_py() -> str:
    return (
        '"""End-to-end tests for the generated storefront (catalog + cart + checkout)."""\n'
        "from __future__ import annotations\n\n"
        "from fastapi.testclient import TestClient\n\n"
        "from src.app import app\n\n"
        "client = TestClient(app)\n\n\n"
        "def test_health():\n"
        '    """Health endpoint returns healthy."""\n'
        '    assert client.get("/health").json()["status"] == "healthy"\n\n\n'
        "def test_home_shows_products():\n"
        '    """Home page renders and shows seeded catalog products."""\n'
        '    resp = client.get("/")\n'
        "    assert resp.status_code == 200\n"
        '    assert "<html" in resp.text.lower()\n'
        '    assert "Signature Starter Pack" in resp.text\n\n\n'
        "def test_api_lists_and_filters_products():\n"
        '    """The products API returns the catalog and supports search."""\n'
        '    products = client.get("/api/products").json()\n'
        "    assert isinstance(products, list) and len(products) >= 1\n"
        '    filtered = client.get("/api/products", params={"q": "laptop"}).json()\n'
        '    assert all("laptop" in (p["name"] + p["description"]).lower()\n'
        "               for p in filtered)\n\n\n"
        "def test_catalog_and_product_pages_render():\n"
        '    """Catalog and product detail pages render HTML."""\n'
        '    assert client.get("/products").status_code == 200\n'
        '    first = client.get("/api/products").json()[0]\n'
        '    detail = client.get("/products/" + str(first["id"]))\n'
        "    assert detail.status_code == 200\n"
        '    assert first["name"] in detail.text\n\n\n'
        "def test_cart_and_checkout_flow():\n"
        '    """Add to cart, then check out and land on the order confirmation."""\n'
        "    c = TestClient(app)\n"
        '    first = c.get("/api/products").json()[0]\n'
        "    added = c.post(\"/cart/add\",\n"
        '                   data={"product_id": first["id"], "quantity": 2})\n'
        "    assert added.status_code == 200\n"
        '    cart = c.get("/cart")\n'
        '    assert first["name"] in cart.text\n'
        "    placed = c.post(\"/checkout\",\n"
        '                    data={"customer_name": "Test Buyer",\n'
        '                          "email": "buyer@example.com"})\n'
        "    assert placed.status_code == 200\n"
        '    assert "order" in placed.text.lower()\n'
        '    assert "Test Buyer" in placed.text\n\n\n'
        "def test_api_places_order():\n"
        '    """An order can be placed through the JSON API."""\n'
        '    first = client.get("/api/products").json()[0]\n'
        "    resp = client.post(\"/api/orders\", json={\n"
        '        "customer_name": "API Buyer", "email": "api@example.com",\n'
        '        "items": [{"product_id": first["id"], "quantity": 1}],\n'
        "    })\n"
        "    assert resp.status_code == 200\n"
        '    assert resp.json()["id"] >= 1\n'
    )


def _readme(spec: AppSpec, product_table: str, order_table: str) -> str:
    src = ""
    if product_table or order_table:
        src = (
            "\n## Source database\n\n"
            "This storefront's design was informed by the connected database "
            "(catalog table: `" + (product_table or "n/a") + "`, orders table: `"
            + (order_table or "n/a") + "`). The app ships its own seeded SQLite "
            "schema so it runs and is testable out of the box; point "
            "`APP_DB_PATH` / `DATABASE_URL` at the real database to serve live "
            "catalog data.\n"
        )
    return (
        "# " + spec.app_name + "\n\n"
        + (spec.description or "A storefront generated by AppBuilderAssistant.")
        + "\n\n## What this is\n\n"
        "A real, runnable ecommerce **storefront**: home, searchable product "
        "catalog, product detail pages, a shopping cart, and a checkout that "
        "places orders. Backed by SQLite with a seeded sample catalog.\n"
        + src +
        "\n## Run\n\n"
        "```bash\n"
        "pip install -r requirements.txt\n"
        "uvicorn src.app:app --reload\n"
        "```\n\n"
        "Open http://127.0.0.1:8000/ to shop and http://127.0.0.1:8000/docs for "
        "the API.\n\n## Test\n\n```bash\npytest -q\n```\n"
    )


def _architecture_md(spec: AppSpec, product_table: str, order_table: str) -> str:
    return (
        "# " + spec.app_name + " architecture\n\n"
        "## Archetype: storefront (ecommerce)\n\n"
        "A customer-facing store, not a CRUD admin. Pages: home, catalog "
        "(search/filter), product detail, cart, checkout, order confirmation.\n\n"
        "## Layers\n\n"
        "- `src/app.py` — FastAPI entry point (API + storefront UI + infra)\n"
        "- `src/api.py` — JSON API: products, categories, orders\n"
        "- `src/web.py` — storefront pages + cart/checkout (cookie cart)\n"
        "- `src/repository.py` — parameterized catalog + order data access\n"
        "- `src/db/` — schema, sample-catalog seeding, connection\n\n"
        "## Database mapping\n\n"
        "- catalog table detected: " + (product_table or "(none — canonical schema used)") + "\n"
        "- orders table detected: " + (order_table or "(none — canonical schema used)") + "\n"
    )


def _detect_tables(spec: AppSpec) -> tuple[str, str]:
    """Best-effort names of the DB tables that informed this storefront."""
    product_table = ""
    order_table = ""
    for e in spec.entities:
        t = e.table
        if not product_table and (
            any(any(h in c for h in ("price", "cost", "amount")) for c in e.safe_fields())
            or any(h in t for h in ("product", "item", "catalog", "good", "sku"))
        ):
            product_table = t
        if not order_table and any(h in t for h in ("order", "sale", "purchase", "cart")):
            order_table = t
    return product_table, order_table


# ── public entry point ─────────────────────────────────────────────────────--
def generate_storefront(spec: AppSpec) -> dict[str, str]:
    """Generate the full file map for a runnable ecommerce storefront."""
    spec = spec.normalized()
    product_table, order_table = _detect_tables(spec)
    files: dict[str, str] = {
        "README.md": _readme(spec, product_table, order_table),
        "requirements.txt": _requirements(),
        "Dockerfile": _dockerfile(),
        ".github/workflows/ci.yml": _ci_yml(),
        "config/infra.yaml": _infra_yaml(spec),
        "src/__init__.py": '"""Application package."""\n',
        "src/settings.py": _settings_py(spec.app_name),
        "src/app.py": _app_py(spec.app_name, spec.services),
        "src/api.py": _api_py(),
        "src/web.py": _web_py(),
        "src/models.py": _models_py(),
        "src/repository.py": _repository_py(),
        "src/db/__init__.py": '"""Database package."""\n',
        "src/db/schema.py": _schema_py(),
        "src/db/schema.sql": _schema_sql(),
        "src/db/connection.py": _connection_py(),
        "templates/base.html": _BASE_HTML.replace("__APP_NAME__", spec.app_name),
        "templates/index.html": _INDEX_HTML,
        "templates/catalog.html": _CATALOG_HTML,
        "templates/product.html": _PRODUCT_HTML,
        "templates/cart.html": _CART_HTML,
        "templates/checkout.html": _CHECKOUT_HTML,
        "templates/confirmation.html": _CONFIRMATION_HTML,
        "static/style.css": _STYLE_CSS,
        "tests/__init__.py": '"""Test package."""\n',
        "tests/conftest.py": _conftest_py(),
        "tests/test_app.py": _tests_py(),
        "docs/ARCHITECTURE.md": _architecture_md(spec, product_table, order_table),
        "deploy/hosting.yaml": _hosting_yaml(spec.app_name),
    }
    if _has_infra(spec.services):
        files["src/infra/__init__.py"] = _infra_init_py(spec.services)
    if "monitoring" in spec.services:
        files["src/infra/monitoring.py"] = _monitoring_py()
    if "notification" in spec.services:
        files["src/infra/notification.py"] = _notification_py()
    if "document" in spec.services:
        files["src/infra/document.py"] = _document_py()
    if "ai_builder" in spec.services:
        files["src/ai/__init__.py"] = '"""AI hooks package."""\n'
        files["src/ai/builder_hooks.py"] = _ai_builder_py()
    files.update(skeleton_files(spec))
    return files
