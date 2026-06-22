"""
MongoDB and AWS DocumentDB connection module (pymongo).

DocumentDB: set tls=True and tls_ca_file to the AWS RDS combined CA bundle.
See: https://www.mongodb.com/docs/drivers/pymongo/
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Optional

from common.config_loader import config, get_db_port, console_debug

from common.drivers.connection_options import DriverConnectionParams

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None  # type: ignore
    PyMongoError = Exception  # type: ignore

MongoError = PyMongoError


class MongoConnection:
    """Wrapper holding client + default database name."""

    __slots__ = ("client", "db_name")

    def __init__(self, client, db_name: str):
        self.client = client
        self.db_name = db_name

    @property
    def db(self):
        return self.client[self.db_name]

    def ping(self):
        self.client.admin.command("ping")

    def close(self):
        self.client.close()


def log(message: str) -> bool:
    """Driver INFO trace — see :func:`common.drivers.conMariadb.log`."""
    logging.info(message)
    console_debug(message)
    return True


def logError(message: str) -> bool:
    logging.error(message)
    print(message, file=sys.stderr)
    return True


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def connectMongo(params=None, **kwargs):
    if MongoClient is None:
        logError("pymongo is not installed. Run: pip install pymongo")
        return None
    params = DriverConnectionParams.from_call(params, kwargs)
    database = params.database
    host = params.host
    user = params.user
    password = params.password
    port = params.port
    tls = params.tls if params.tls is not None else False
    tls_ca_file = params.tls_ca_file
    auth_source = params.auth_source
    if port is None:
        port = get_db_port("mongodb")
    use_tls = _as_bool(tls)
    try:
        timeout_ms = max(
            1000,
            int(config.get_float("database.connection", "connection_timeout", default=30.0) * 1000),
        )
        kwargs: dict[str, Any] = {
            "host": host,
            "port": int(port),
            "username": user or None,
            "password": password or None,
            "tls": use_tls,
            "serverSelectionTimeoutMS": timeout_ms,
        }
        if auth_source:
            kwargs["authSource"] = auth_source
        elif user:
            kwargs["authSource"] = database or "admin"
        if use_tls and tls_ca_file:
            kwargs["tlsCAFile"] = tls_ca_file
        client = MongoClient(**kwargs)
        client.admin.command("ping")
        db_name = database or "admin"
        log(f"Connected to MongoDB: {db_name}@{host}:{port} tls={use_tls}")
        return MongoConnection(client, db_name)
    except MongoError as e:
        logError(f"Failed to connect to MongoDB: {e}")
        return None


def connectDocumentDB(params=None, **kwargs):
    """DocumentDB uses TLS; delegate to connectMongo with tls enabled."""
    params = DriverConnectionParams.from_call(params, kwargs)
    return connectMongo(
        params=DriverConnectionParams(
            database=params.database,
            host=params.host,
            user=params.user,
            password=params.password,
            port=params.port or get_db_port("documentdb"),
        ),
        tls=True,
        tls_ca_file=params.tls_ca_file,
        auth_source=params.auth_source or params.database,
    )


def disconnectMongo(conn):
    if conn:
        conn.close()
        log("Disconnected from MongoDB")
    return True


def pingMongo(conn):
    try:
        if not conn:
            return False
        conn.ping()
        return True
    except Exception:
        return False


def reconnectMongo(conn, params=None, **kwargs):
    try:
        disconnectMongo(conn)
    except Exception:
        pass
    return connectMongo(
        params=DriverConnectionParams.from_call(params, kwargs),
    )


def getMongoVersion(conn):
    try:
        info = conn.client.server_info()
        return info.get("version")
    except MongoError as e:
        logError(f"Failed to get MongoDB version: {e}")
        return None


def isRoot(conn):
    return False


def getCurrentDatabase(conn):
    return conn.db_name if conn else None


def getMongoCollections(conn):
    try:
        return sorted(conn.db.list_collection_names())
    except MongoError as e:
        logError(f"Failed to list collections: {e}")
        return []


def getMongoIndexes(conn):
    try:
        names = []
        for coll in conn.db.list_collection_names():
            for idx in conn.db[coll].list_indexes():
                names.append(f"{coll}.{idx.get('name', 'index')}")
        return sorted(names)
    except MongoError as e:
        logError(f"Failed to list indexes: {e}")
        return []


def getMongoUsers(conn):
    try:
        users = conn.client[conn.db_name].command("usersInfo")
        return sorted(u.get("user", "") for u in users.get("users", []))
    except MongoError:
        return []


def getMongoTableSchema(conn, collection_name):
    """Infer field names/types from sample documents and JSON schema if present."""
    try:
        coll = conn.db[collection_name]
        sample = coll.find_one() or {}
        columns = []
        for key, val in sample.items():
            columns.append(
                {
                    "name": key,
                    "type": type(val).__name__,
                    "nullable": True,
                    "default": None,
                }
            )
        return columns
    except MongoError as e:
        logError(f"Failed to describe collection {collection_name}: {e}")
        return []


def executeMongoQuery(conn, query_text: str) -> tuple[Optional[dict], Optional[str]]:
    """
    Run a JSON document query.

    Example find::
        {"collection": "users", "operation": "find", "filter": {}, "limit": 50}

    Example aggregate::
        {"collection": "orders", "operation": "aggregate",
         "pipeline": [{"$match": {"status": "open"}}], "limit": 50}
    """
    try:
        payload = json.loads(query_text)
    except json.JSONDecodeError as exc:
        return None, f"Invalid JSON query: {exc}"

    collection = (payload.get("collection") or "").strip()
    if not collection:
        return None, "Document query requires 'collection'."

    operation = (payload.get("operation") or "find").strip().lower()
    limit_raw = payload.get("limit")
    coll = conn.db[collection]

    if operation == "find":
        cursor = coll.find(
            payload.get("filter") or {},
            payload.get("projection"),
        )
        sort = payload.get("sort")
        if sort:
            cursor = cursor.sort(list(sort.items()) if isinstance(sort, dict) else sort)
        if limit_raw is None:
            docs = list(cursor)
        else:
            limit = int(limit_raw or 100)
            docs = list(cursor.limit(max(1, min(limit, 1000))))
        if not docs:
            return {"columns": ["(empty)"], "rows": [], "rowcount": 0}, None
        columns = sorted({k for doc in docs for k in doc.keys()})
        rows = [[_stringify(doc.get(c)) for c in columns] for doc in docs]
        return {"columns": columns, "rows": rows, "rowcount": len(rows)}, None

    if operation == "aggregate":
        pipeline = payload.get("pipeline") or []
        if limit_raw is not None:
            agg_limit = int(limit_raw or 100)
            pipeline = list(pipeline) + [{"$limit": max(1, min(agg_limit, 1000))}]
        docs = list(coll.aggregate(pipeline))
        if not docs:
            return {"columns": ["(empty)"], "rows": [], "rowcount": 0}, None
        columns = sorted({k for doc in docs for k in doc.keys()})
        rows = [[_stringify(doc.get(c)) for c in columns] for doc in docs]
        return {"columns": columns, "rows": rows, "rowcount": len(rows)}, None

    if operation == "count":
        count = coll.count_documents(payload.get("filter") or {})
        return {
            "columns": ["count"],
            "rows": [[count]],
            "rowcount": 1,
            "message": f"count = {count}",
        }, None

    return None, f"Unsupported operation '{operation}'. Use find, aggregate, or count."


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def _serialize_document(doc: dict) -> dict:
    """Return a BSON-safe copy of *doc* for insert_many."""
    out = {}
    for key, val in doc.items():
        if isinstance(val, (dict, list)):
            out[key] = json.loads(json.dumps(val, default=str))
        else:
            out[key] = val
    return out


def readMongoDocuments(conn, collection_name, batch_size=1000, skip=0):
    """Read a batch of documents from a collection."""
    try:
        coll = conn.db[collection_name]
        cursor = coll.find({}).skip(int(skip or 0)).limit(max(1, int(batch_size or 1000)))
        return [_serialize_document(doc) for doc in cursor]
    except MongoError as e:
        logError(f"Failed to read collection {collection_name}: {e}")
        return []


def insertMongoDocuments(conn, collection_name, documents):
    """Insert many documents into a collection. Returns inserted count."""
    if not documents:
        return 0
    try:
        coll = conn.db[collection_name]
        payload = [_serialize_document(doc) for doc in documents]
        result = coll.insert_many(payload)
        return len(result.inserted_ids)
    except MongoError as e:
        logError(f"Failed to insert into collection {collection_name}: {e}")
        raise


def createMongoCollection(conn, collection_name):
    """Create a collection if it does not exist."""
    try:
        if collection_name not in conn.db.list_collection_names():
            conn.db.create_collection(collection_name)
        return True
    except MongoError as e:
        logError(f"Failed to create collection {collection_name}: {e}")
        return False


def dropMongoCollection(conn, collection_name):
    """Drop a collection."""
    try:
        conn.db.drop_collection(collection_name)
        return True
    except MongoError as e:
        logError(f"Failed to drop collection {collection_name}: {e}")
        return False
