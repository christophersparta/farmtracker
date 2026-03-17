"""db.py — Pure sqlite3 database layer for FarmTracker."""
import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "farmtracker.db"))


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_conn():
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'worker',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS crops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    category TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS fields (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    size_ha REAL,
    gps_lat REAL,
    gps_lon REAL,
    soil_type TEXT,
    crop_id INTEGER REFERENCES crops(id),
    planting_date TEXT,
    expected_harvest_date TEXT,
    status TEXT DEFAULT 'idle',
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS inventory_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT,
    unit TEXT,
    qty_on_hand REAL DEFAULT 0,
    reorder_threshold REAL DEFAULT 0,
    cost_per_unit REAL DEFAULT 0,
    supplier TEXT,
    notes TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS harvests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    field_id INTEGER REFERENCES fields(id),
    crop_id INTEGER REFERENCES crops(id),
    date TEXT NOT NULL,
    qty REAL NOT NULL,
    unit TEXT DEFAULT 'kg',
    quality TEXT,
    storage_location TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_id TEXT UNIQUE,
    harvest_id INTEGER REFERENCES harvests(id),
    crop_id INTEGER REFERENCES crops(id),
    buyer TEXT,
    qty REAL NOT NULL,
    unit TEXT DEFAULT 'kg',
    price_per_unit REAL NOT NULL,
    total REAL,
    date TEXT NOT NULL,
    payment_status TEXT DEFAULT 'pending',
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS beehives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hive_code TEXT UNIQUE NOT NULL,
    location_field_id INTEGER REFERENCES fields(id),
    queen_date TEXT,
    last_inspection_date TEXT,
    health_status TEXT DEFAULT 'healthy',
    is_producing INTEGER DEFAULT 1,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS honey_harvests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hive_id INTEGER REFERENCES beehives(id),
    date TEXT NOT NULL,
    qty_liters REAL NOT NULL,
    quality TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    field_id INTEGER REFERENCES fields(id),
    assigned_user_id INTEGER REFERENCES users(id),
    task_type TEXT,
    description TEXT,
    date TEXT NOT NULL,
    hours REAL DEFAULT 0,
    cost REAL DEFAULT 0,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    category TEXT,
    amount REAL NOT NULL,
    description TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""


def init_db():
    with db_conn() as conn:
        conn.executescript(SCHEMA)
