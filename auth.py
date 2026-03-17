"""auth.py — Password hashing and session helpers (no external deps)."""
import hashlib
import os
import secrets
from functools import wraps
from flask import session, redirect, url_for, flash, g
from db import db_conn


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000)
    return f"{salt}:{h.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt, h = stored.split(":", 1)
        check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000)
        return check.hex() == h
    except Exception:
        return False


def get_current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=? AND is_active=1", (uid,)).fetchone()
        return dict(row) if row else None


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = get_current_user()
        if not user:
            flash("Please log in.", "warning")
            return redirect(url_for("auth_login"))
        g.user = user
        return fn(*args, **kwargs)
    return wrapper


def role_required(*roles):
    def decorator(fn):
        @wraps(fn)
        @login_required
        def wrapper(*args, **kwargs):
            if g.user["role"] not in roles:
                flash("Access denied.", "danger")
                return redirect(url_for("dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator
