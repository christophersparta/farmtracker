"""
FarmTracker — Flask + SQLite web app
No external ORM or auth libraries required beyond Flask itself.
"""
import csv
import io
import json
import os
import secrets
from datetime import date, datetime, timedelta
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, g, jsonify, Response, send_file)

from db import db_conn, init_db
from auth import hash_password, verify_password, get_current_user, login_required, role_required

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-farmtracker-2026")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024

# ── Helpers ──────────────────────────────────────────────────────────────────

def fmt_mk(val):
    try:
        return f"MK {float(val):,.0f}"
    except Exception:
        return "MK 0"

app.jinja_env.globals["fmt_mk"] = fmt_mk
app.jinja_env.globals["now"] = datetime.now


@app.before_request
def load_user():
    g.user = get_current_user()


@app.context_processor
def inject_user():
    return {"current_user": g.get("user")}


# ── AUTH ─────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def auth_login():
    if g.user:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        pw = request.form.get("password", "")
        with db_conn() as conn:
            user = conn.execute("SELECT * FROM users WHERE email=? AND is_active=1", (email,)).fetchone()
        if user and verify_password(pw, user["password_hash"]):
            session["user_id"] = user["id"]
            return redirect(url_for("dashboard"))
        flash("Invalid email or password.", "danger")
    return render_template("auth/login.html")


@app.route("/logout")
def auth_logout():
    session.clear()
    return redirect(url_for("auth_login"))


@app.route("/users")
@login_required
def users_list():
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("dashboard"))
    with db_conn() as conn:
        users = conn.execute("SELECT * FROM users ORDER BY name").fetchall()
    return render_template("auth/users.html", users=[dict(u) for u in users])


@app.route("/users/new", methods=["GET", "POST"])
@login_required
def users_new():
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        name = request.form["name"].strip()
        email = request.form["email"].strip().lower()
        pw = request.form["password"]
        role = request.form.get("role", "worker")
        with db_conn() as conn:
            existing = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
            if existing:
                flash("Email already registered.", "warning")
            else:
                conn.execute("INSERT INTO users (name,email,password_hash,role) VALUES (?,?,?,?)",
                             (name, email, hash_password(pw), role))
                flash(f"User {name} created.", "success")
                return redirect(url_for("users_list"))
    return render_template("auth/new_user.html")


@app.route("/users/<int:uid>/toggle")
@login_required
def users_toggle(uid):
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("users_list"))
    if uid == g.user["id"]:
        flash("Cannot deactivate yourself.", "warning")
    else:
        with db_conn() as conn:
            u = conn.execute("SELECT is_active FROM users WHERE id=?", (uid,)).fetchone()
            if u:
                conn.execute("UPDATE users SET is_active=? WHERE id=?", (0 if u["is_active"] else 1, uid))
                flash("User status updated.", "success")
    return redirect(url_for("users_list"))


# ── DASHBOARD ─────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    today = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    in_14 = (date.today() + timedelta(days=14)).isoformat()

    with db_conn() as conn:
        today_sales = conn.execute(
            "SELECT COALESCE(SUM(total),0) FROM sales WHERE date=?", (today,)).fetchone()[0]
        month_sales = conn.execute(
            "SELECT COALESCE(SUM(total),0) FROM sales WHERE date>=?", (month_start,)).fetchone()[0]
        stock_val = conn.execute(
            "SELECT COALESCE(SUM(qty_on_hand*cost_per_unit),0) FROM inventory_items").fetchone()[0]
        low_stock_count = conn.execute(
            "SELECT COUNT(*) FROM inventory_items WHERE qty_on_hand<=reorder_threshold").fetchone()[0]
        active_fields = conn.execute(
            "SELECT COUNT(*) FROM fields WHERE status IN ('planted','growing')").fetchone()[0]
        pending_tasks = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status IN ('pending','in_progress')").fetchone()[0]
        producing_hives = conn.execute(
            "SELECT COUNT(*) FROM beehives WHERE is_producing=1").fetchone()[0]
        total_hives = conn.execute("SELECT COUNT(*) FROM beehives").fetchone()[0]

        recent_sales = conn.execute("""
            SELECT s.*, c.display_name as crop_name FROM sales s
            LEFT JOIN crops c ON s.crop_id=c.id
            ORDER BY s.date DESC LIMIT 8""").fetchall()

        _uh_rows = conn.execute("""
            SELECT f.*, c.display_name as crop_name FROM fields f
            LEFT JOIN crops c ON f.crop_id=c.id
            WHERE f.expected_harvest_date IS NOT NULL
              AND f.expected_harvest_date >= ? AND f.expected_harvest_date <= ?
            ORDER BY f.expected_harvest_date LIMIT 6""", (today, in_14)).fetchall()
        upcoming_harvests = []
        for _r in _uh_rows:
            _d = dict(_r)
            try:
                _d["days_to_harvest"] = (date.fromisoformat(_d["expected_harvest_date"]) - date.today()).days
            except Exception:
                _d["days_to_harvest"] = None
            upcoming_harvests.append(_d)

        low_stock = conn.execute("""
            SELECT * FROM inventory_items WHERE qty_on_hand<=reorder_threshold
            ORDER BY qty_on_hand LIMIT 8""").fetchall()

        # Monthly yield per crop (last 6 months)
        crops_all = conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()
        monthly_yield = {}
        for i in range(5, -1, -1):
            d = date.today().replace(day=1) - timedelta(days=i * 28)
            label = d.strftime("%b")
            monthly_yield[label] = {}
            ym = d.strftime("%Y-%m")
            for c in crops_all:
                row = conn.execute("""
                    SELECT COALESCE(SUM(qty),0) FROM harvests
                    WHERE crop_id=? AND strftime('%Y-%m', date)=?""", (c["id"], ym)).fetchone()
                monthly_yield[label][c["display_name"]] = round(float(row[0]), 1)

        # Sales by crop
        sales_by_crop = conn.execute("""
            SELECT c.display_name, COALESCE(SUM(s.total),0) as total
            FROM crops c LEFT JOIN sales s ON s.crop_id=c.id
            GROUP BY c.id ORDER BY total DESC""").fetchall()

    kpis = {
        "today_sales": round(float(today_sales), 2),
        "month_sales": round(float(month_sales), 2),
        "stock_value": round(float(stock_val), 2),
        "low_stock_count": low_stock_count,
        "active_fields": active_fields,
        "pending_tasks": pending_tasks,
        "producing_hives": producing_hives,
        "total_hives": total_hives,
    }
    return render_template("dashboard/index.html",
        kpis=kpis,
        recent_sales=[dict(r) for r in recent_sales],
        upcoming_harvests=upcoming_harvests,
        low_stock=[dict(r) for r in low_stock],
        monthly_yield=monthly_yield,
        sales_by_crop=[dict(r) for r in sales_by_crop],
    )


# ── CROPS ─────────────────────────────────────────────────────────────────────

@app.route("/crops")
@login_required
def crops_list():
    with db_conn() as conn:
        crops = conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()
    return render_template("crops/index.html", crops=[dict(c) for c in crops])


@app.route("/crops/new", methods=["GET", "POST"])
@login_required
def crops_new():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("crops_list"))
    if request.method == "POST":
        slug = request.form["slug"].strip().lower().replace(" ", "_")
        try:
            with db_conn() as conn:
                conn.execute("INSERT INTO crops (slug,display_name,category,notes) VALUES (?,?,?,?)",
                    (slug, request.form["display_name"].strip(),
                     request.form.get("category", ""), request.form.get("notes", "")))
            flash("Crop added.", "success")
            return redirect(url_for("crops_list"))
        except Exception as e:
            flash(f"Could not add crop: {e}", "danger")
    return render_template("crops/form.html", crop=None)


@app.route("/crops/<int:cid>/edit", methods=["GET", "POST"])
@login_required
def crops_edit(cid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("crops_list"))
    with db_conn() as conn:
        crop = dict(conn.execute("SELECT * FROM crops WHERE id=?", (cid,)).fetchone())
        if request.method == "POST":
            conn.execute("UPDATE crops SET display_name=?,category=?,notes=? WHERE id=?",
                (request.form["display_name"].strip(),
                 request.form.get("category", ""), request.form.get("notes", ""), cid))
            flash("Crop updated.", "success")
            return redirect(url_for("crops_list"))
    return render_template("crops/form.html", crop=crop)


@app.route("/crops/<int:cid>/delete", methods=["POST"])
@login_required
def crops_delete(cid):
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("crops_list"))
    with db_conn() as conn:
        conn.execute("DELETE FROM crops WHERE id=?", (cid,))
    flash("Crop deleted.", "success")
    return redirect(url_for("crops_list"))


@app.route("/crops/import", methods=["POST"])
@login_required
def crops_import():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("crops_list"))
    f = request.files.get("file")
    if not f:
        flash("No file uploaded.", "warning")
        return redirect(url_for("crops_list"))
    stream = io.StringIO(f.stream.read().decode("utf-8"))
    count = 0
    with db_conn() as conn:
        for row in csv.DictReader(stream):
            slug = row.get("slug", "").strip().lower()
            if not slug:
                continue
            existing = conn.execute("SELECT id FROM crops WHERE slug=?", (slug,)).fetchone()
            if not existing:
                conn.execute("INSERT INTO crops (slug,display_name,category,notes) VALUES (?,?,?,?)",
                    (slug, row.get("display_name", slug), row.get("category", ""), row.get("notes", "")))
                count += 1
    flash(f"Imported {count} crop(s).", "success")
    return redirect(url_for("crops_list"))


@app.route("/crops/export")
@login_required
def crops_export():
    with db_conn() as conn:
        crops = conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id", "slug", "display_name", "category", "notes"])
    for c in crops:
        w.writerow([c["id"], c["slug"], c["display_name"], c["category"], c["notes"] or ""])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=crops.csv"})


# ── FIELDS ────────────────────────────────────────────────────────────────────

@app.route("/fields")
@login_required
def fields_list():
    q = request.args.get("q", "")
    status = request.args.get("status", "")
    today = date.today().isoformat()
    with db_conn() as conn:
        sql = """SELECT f.*, c.display_name as crop_name FROM fields f
                 LEFT JOIN crops c ON f.crop_id=c.id WHERE 1=1"""
        params = []
        if q:
            sql += " AND f.name LIKE ?"
            params.append(f"%{q}%")
        if status:
            sql += " AND f.status=?"
            params.append(status)
        sql += " ORDER BY f.name"
        fields = [dict(r) for r in conn.execute(sql, params).fetchall()]
    for f in fields:
        if f.get("expected_harvest_date"):
            try:
                d = date.fromisoformat(f["expected_harvest_date"])
                f["days_to_harvest"] = (d - date.today()).days
            except Exception:
                f["days_to_harvest"] = None
        else:
            f["days_to_harvest"] = None
    return render_template("fields/index.html", fields=fields, q=q, status=status)


@app.route("/fields/new", methods=["GET", "POST"])
@login_required
def fields_new():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("fields_list"))
    with db_conn() as conn:
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
    if request.method == "POST":
        with db_conn() as conn:
            conn.execute("""INSERT INTO fields
                (name,size_ha,gps_lat,gps_lon,soil_type,crop_id,planting_date,expected_harvest_date,status,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?)""", (
                request.form["name"].strip(),
                float(request.form.get("size_ha") or 0),
                float(request.form["gps_lat"]) if request.form.get("gps_lat") else None,
                float(request.form["gps_lon"]) if request.form.get("gps_lon") else None,
                request.form.get("soil_type", ""),
                int(request.form["crop_id"]) if request.form.get("crop_id") else None,
                request.form.get("planting_date") or None,
                request.form.get("expected_harvest_date") or None,
                request.form.get("status", "idle"),
                request.form.get("notes", ""),
            ))
        flash("Field added.", "success")
        return redirect(url_for("fields_list"))
    return render_template("fields/form.html", field=None, crops=crops)


@app.route("/fields/<int:fid>/edit", methods=["GET", "POST"])
@login_required
def fields_edit(fid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("fields_list"))
    with db_conn() as conn:
        field = dict(conn.execute("SELECT * FROM fields WHERE id=?", (fid,)).fetchone())
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
        if request.method == "POST":
            conn.execute("""UPDATE fields SET name=?,size_ha=?,gps_lat=?,gps_lon=?,soil_type=?,
                crop_id=?,planting_date=?,expected_harvest_date=?,status=?,notes=? WHERE id=?""", (
                request.form["name"].strip(),
                float(request.form.get("size_ha") or 0),
                float(request.form["gps_lat"]) if request.form.get("gps_lat") else None,
                float(request.form["gps_lon"]) if request.form.get("gps_lon") else None,
                request.form.get("soil_type", ""),
                int(request.form["crop_id"]) if request.form.get("crop_id") else None,
                request.form.get("planting_date") or None,
                request.form.get("expected_harvest_date") or None,
                request.form.get("status", "idle"),
                request.form.get("notes", ""), fid,
            ))
            flash("Field updated.", "success")
            return redirect(url_for("fields_list"))
    return render_template("fields/form.html", field=field, crops=crops)


@app.route("/fields/<int:fid>/delete", methods=["POST"])
@login_required
def fields_delete(fid):
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("fields_list"))
    with db_conn() as conn:
        conn.execute("DELETE FROM fields WHERE id=?", (fid,))
    flash("Field deleted.", "success")
    return redirect(url_for("fields_list"))


# ── INVENTORY ─────────────────────────────────────────────────────────────────

@app.route("/inventory")
@login_required
def inventory_list():
    q = request.args.get("q", "")
    show_low = request.args.get("low", "")
    with db_conn() as conn:
        sql = "SELECT * FROM inventory_items WHERE 1=1"
        params = []
        if q:
            sql += " AND name LIKE ?"
            params.append(f"%{q}%")
        if show_low:
            sql += " AND qty_on_hand<=reorder_threshold"
        sql += " ORDER BY name"
        items = [dict(r) for r in conn.execute(sql, params).fetchall()]
    for i in items:
        i["stock_value"] = round(i["qty_on_hand"] * i["cost_per_unit"], 2)
        i["is_low"] = i["qty_on_hand"] <= i["reorder_threshold"]
    return render_template("inventory/index.html", items=items, q=q, show_low=show_low)


@app.route("/inventory/new", methods=["GET", "POST"])
@login_required
def inventory_new():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("inventory_list"))
    if request.method == "POST":
        with db_conn() as conn:
            conn.execute("""INSERT INTO inventory_items
                (name,category,unit,qty_on_hand,reorder_threshold,cost_per_unit,supplier,notes)
                VALUES (?,?,?,?,?,?,?,?)""", (
                request.form["name"].strip(),
                request.form.get("category", ""),
                request.form.get("unit", ""),
                float(request.form.get("qty_on_hand") or 0),
                float(request.form.get("reorder_threshold") or 0),
                float(request.form.get("cost_per_unit") or 0),
                request.form.get("supplier", ""),
                request.form.get("notes", ""),
            ))
        flash("Item added.", "success")
        return redirect(url_for("inventory_list"))
    return render_template("inventory/form.html", item=None)


@app.route("/inventory/<int:iid>/edit", methods=["GET", "POST"])
@login_required
def inventory_edit(iid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("inventory_list"))
    with db_conn() as conn:
        item = dict(conn.execute("SELECT * FROM inventory_items WHERE id=?", (iid,)).fetchone())
        if request.method == "POST":
            conn.execute("""UPDATE inventory_items SET name=?,category=?,unit=?,qty_on_hand=?,
                reorder_threshold=?,cost_per_unit=?,supplier=?,notes=?,
                updated_at=datetime('now') WHERE id=?""", (
                request.form["name"].strip(),
                request.form.get("category", ""),
                request.form.get("unit", ""),
                float(request.form.get("qty_on_hand") or 0),
                float(request.form.get("reorder_threshold") or 0),
                float(request.form.get("cost_per_unit") or 0),
                request.form.get("supplier", ""),
                request.form.get("notes", ""), iid,
            ))
            flash("Item updated.", "success")
            return redirect(url_for("inventory_list"))
    return render_template("inventory/form.html", item=item)


@app.route("/inventory/<int:iid>/qty", methods=["POST"])
@login_required
def inventory_update_qty(iid):
    """Inline qty AJAX update."""
    if g.user["role"] not in ("admin", "manager"):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    data = request.get_json() or {}
    try:
        qty = float(data.get("qty", 0))
        with db_conn() as conn:
            conn.execute("UPDATE inventory_items SET qty_on_hand=?,updated_at=datetime('now') WHERE id=?", (qty, iid))
            row = conn.execute("SELECT qty_on_hand,reorder_threshold FROM inventory_items WHERE id=?", (iid,)).fetchone()
        is_low = row["qty_on_hand"] <= row["reorder_threshold"]
        return jsonify({"ok": True, "qty": qty, "is_low": is_low})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/inventory/<int:iid>/delete", methods=["POST"])
@login_required
def inventory_delete(iid):
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("inventory_list"))
    with db_conn() as conn:
        conn.execute("DELETE FROM inventory_items WHERE id=?", (iid,))
    flash("Item deleted.", "success")
    return redirect(url_for("inventory_list"))


@app.route("/inventory/import", methods=["POST"])
@login_required
def inventory_import():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("inventory_list"))
    f = request.files.get("file")
    if not f:
        flash("No file uploaded.", "warning")
        return redirect(url_for("inventory_list"))
    stream = io.StringIO(f.stream.read().decode("utf-8"))
    count = 0
    with db_conn() as conn:
        for row in csv.DictReader(stream):
            name = row.get("name", "").strip()
            if not name:
                continue
            conn.execute("""INSERT INTO inventory_items
                (name,category,unit,qty_on_hand,reorder_threshold,cost_per_unit,supplier)
                VALUES (?,?,?,?,?,?,?)""", (
                name, row.get("category",""), row.get("unit",""),
                float(row.get("qty_on_hand") or 0), float(row.get("reorder_threshold") or 0),
                float(row.get("cost_per_unit") or 0), row.get("supplier",""),
            ))
            count += 1
    flash(f"Imported {count} item(s).", "success")
    return redirect(url_for("inventory_list"))


@app.route("/inventory/export")
@login_required
def inventory_export():
    with db_conn() as conn:
        items = conn.execute("SELECT * FROM inventory_items ORDER BY name").fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id","name","category","unit","qty_on_hand","reorder_threshold","cost_per_unit","supplier"])
    for i in items:
        w.writerow([i["id"],i["name"],i["category"],i["unit"],i["qty_on_hand"],i["reorder_threshold"],i["cost_per_unit"],i["supplier"] or ""])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=inventory.csv"})


# ── HARVESTS ──────────────────────────────────────────────────────────────────

@app.route("/harvests")
@login_required
def harvests_list():
    crop_filter = request.args.get("crop_id", "")
    with db_conn() as conn:
        sql = """SELECT h.*, c.display_name as crop_name, f.name as field_name
                 FROM harvests h LEFT JOIN crops c ON h.crop_id=c.id
                 LEFT JOIN fields f ON h.field_id=f.id WHERE 1=1"""
        params = []
        if crop_filter:
            sql += " AND h.crop_id=?"
            params.append(int(crop_filter))
        sql += " ORDER BY h.date DESC"
        harvests = [dict(r) for r in conn.execute(sql, params).fetchall()]
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
    return render_template("harvests/index.html", harvests=harvests, crops=crops, selected_crop=crop_filter)


@app.route("/harvests/new", methods=["GET", "POST"])
@login_required
def harvests_new():
    with db_conn() as conn:
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
    if request.method == "POST":
        with db_conn() as conn:
            conn.execute("""INSERT INTO harvests (field_id,crop_id,date,qty,unit,quality,storage_location,notes)
                VALUES (?,?,?,?,?,?,?,?)""", (
                int(request.form["field_id"]),
                int(request.form["crop_id"]),
                request.form["date"],
                float(request.form["qty"]),
                request.form.get("unit", "kg"),
                request.form.get("quality", ""),
                request.form.get("storage_location", ""),
                request.form.get("notes", ""),
            ))
            conn.execute("UPDATE fields SET status='harvested' WHERE id=?", (request.form["field_id"],))
        flash("Harvest logged.", "success")
        return redirect(url_for("harvests_list"))
    return render_template("harvests/form.html", harvest=None, crops=crops, fields=fields)


@app.route("/harvests/<int:hid>/edit", methods=["GET", "POST"])
@login_required
def harvests_edit(hid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("harvests_list"))
    with db_conn() as conn:
        harvest = dict(conn.execute("SELECT * FROM harvests WHERE id=?", (hid,)).fetchone())
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
        if request.method == "POST":
            conn.execute("""UPDATE harvests SET field_id=?,crop_id=?,date=?,qty=?,unit=?,quality=?,
                storage_location=?,notes=? WHERE id=?""", (
                int(request.form["field_id"]), int(request.form["crop_id"]),
                request.form["date"], float(request.form["qty"]),
                request.form.get("unit","kg"), request.form.get("quality",""),
                request.form.get("storage_location",""), request.form.get("notes",""), hid,
            ))
            flash("Harvest updated.", "success")
            return redirect(url_for("harvests_list"))
    return render_template("harvests/form.html", harvest=harvest, crops=crops, fields=fields)


@app.route("/harvests/<int:hid>/delete", methods=["POST"])
@login_required
def harvests_delete(hid):
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("harvests_list"))
    with db_conn() as conn:
        conn.execute("DELETE FROM harvests WHERE id=?", (hid,))
    flash("Harvest deleted.", "success")
    return redirect(url_for("harvests_list"))


@app.route("/harvests/export")
@login_required
def harvests_export():
    with db_conn() as conn:
        rows = conn.execute("""SELECT h.*,c.display_name,f.name as fname FROM harvests h
            LEFT JOIN crops c ON h.crop_id=c.id LEFT JOIN fields f ON h.field_id=f.id
            ORDER BY h.date DESC""").fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id","date","crop","field","qty","unit","quality","storage"])
    for r in rows:
        w.writerow([r["id"],r["date"],r["display_name"],r["fname"],r["qty"],r["unit"],r["quality"] or "",r["storage_location"] or ""])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=harvests.csv"})


# ── SALES ─────────────────────────────────────────────────────────────────────

@app.route("/sales")
@login_required
def sales_list():
    crop_id = request.args.get("crop_id", "")
    status = request.args.get("status", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")
    with db_conn() as conn:
        sql = """SELECT s.*,c.display_name as crop_name FROM sales s
                 LEFT JOIN crops c ON s.crop_id=c.id WHERE 1=1"""
        params = []
        if crop_id:
            sql += " AND s.crop_id=?"; params.append(int(crop_id))
        if status:
            sql += " AND s.payment_status=?"; params.append(status)
        if date_from:
            sql += " AND s.date>=?"; params.append(date_from)
        if date_to:
            sql += " AND s.date<=?"; params.append(date_to)
        sql += " ORDER BY s.date DESC"
        sales = [dict(r) for r in conn.execute(sql, params).fetchall()]
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
    total_filtered = sum(s.get("total") or 0 for s in sales)
    return render_template("sales/index.html", sales=sales, crops=crops,
                           crop_id=crop_id, status=status, date_from=date_from, date_to=date_to,
                           total_filtered=total_filtered)


@app.route("/sales/new", methods=["GET", "POST"])
@login_required
def sales_new():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("sales_list"))
    with db_conn() as conn:
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
    if request.method == "POST":
        qty = float(request.form["qty"])
        price = float(request.form["price_per_unit"])
        total = round(qty * price, 2)
        sale_date = request.form["date"]
        with db_conn() as conn:
            cur = conn.execute("""INSERT INTO sales
                (crop_id,buyer,qty,unit,price_per_unit,total,date,payment_status,notes)
                VALUES (?,?,?,?,?,?,?,?,?)""", (
                int(request.form["crop_id"]),
                request.form.get("buyer",""),
                qty, request.form.get("unit","kg"),
                price, total, sale_date,
                request.form.get("payment_status","pending"),
                request.form.get("notes",""),
            ))
            sid = cur.lastrowid
            inv = f"INV-{sale_date.replace('-','')}-{sid:04d}"
            conn.execute("UPDATE sales SET invoice_id=? WHERE id=?", (inv, sid))
        flash(f"Sale {inv} recorded.", "success")
        return redirect(url_for("sales_list"))
    return render_template("sales/form.html", sale=None, crops=crops)


@app.route("/sales/<int:sid>/edit", methods=["GET", "POST"])
@login_required
def sales_edit(sid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("sales_list"))
    with db_conn() as conn:
        sale = dict(conn.execute("SELECT * FROM sales WHERE id=?", (sid,)).fetchone())
        crops = [dict(c) for c in conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()]
        if request.method == "POST":
            qty = float(request.form["qty"])
            price = float(request.form["price_per_unit"])
            conn.execute("""UPDATE sales SET crop_id=?,buyer=?,qty=?,unit=?,price_per_unit=?,
                total=?,date=?,payment_status=?,notes=? WHERE id=?""", (
                int(request.form["crop_id"]), request.form.get("buyer",""),
                qty, request.form.get("unit","kg"), price, round(qty*price,2),
                request.form["date"], request.form.get("payment_status","pending"),
                request.form.get("notes",""), sid,
            ))
            flash("Sale updated.", "success")
            return redirect(url_for("sales_list"))
    return render_template("sales/form.html", sale=sale, crops=crops)


@app.route("/sales/<int:sid>/delete", methods=["POST"])
@login_required
def sales_delete(sid):
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("sales_list"))
    with db_conn() as conn:
        conn.execute("DELETE FROM sales WHERE id=?", (sid,))
    flash("Sale deleted.", "success")
    return redirect(url_for("sales_list"))


@app.route("/sales/export")
@login_required
def sales_export():
    with db_conn() as conn:
        rows = conn.execute("""SELECT s.*,c.display_name FROM sales s
            LEFT JOIN crops c ON s.crop_id=c.id ORDER BY s.date DESC""").fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["invoice_id","date","crop","buyer","qty","unit","price_per_unit","total","status"])
    for r in rows:
        w.writerow([r["invoice_id"],r["date"],r["display_name"],r["buyer"],r["qty"],r["unit"],r["price_per_unit"],r["total"],r["payment_status"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=sales.csv"})


# ── TASKS ─────────────────────────────────────────────────────────────────────

@app.route("/tasks")
@login_required
def tasks_list():
    status = request.args.get("status", "")
    with db_conn() as conn:
        sql = """SELECT t.*,f.name as field_name,u.name as worker_name FROM tasks t
                 LEFT JOIN fields f ON t.field_id=f.id LEFT JOIN users u ON t.assigned_user_id=u.id
                 WHERE 1=1"""
        params = []
        if status:
            sql += " AND t.status=?"; params.append(status)
        sql += " ORDER BY t.date DESC"
        tasks = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return render_template("tasks/index.html", tasks=tasks, status=status)


@app.route("/tasks/new", methods=["GET", "POST"])
@login_required
def tasks_new():
    with db_conn() as conn:
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
        workers = [dict(u) for u in conn.execute("SELECT * FROM users WHERE role IN ('worker','manager') AND is_active=1 ORDER BY name").fetchall()]
    if request.method == "POST":
        with db_conn() as conn:
            conn.execute("""INSERT INTO tasks (field_id,assigned_user_id,task_type,description,date,hours,cost,status)
                VALUES (?,?,?,?,?,?,?,?)""", (
                int(request.form["field_id"]) if request.form.get("field_id") else None,
                int(request.form["assigned_user_id"]) if request.form.get("assigned_user_id") else None,
                request.form.get("task_type",""),
                request.form.get("description",""),
                request.form["date"],
                float(request.form.get("hours") or 0),
                float(request.form.get("cost") or 0),
                request.form.get("status","pending"),
            ))
        flash("Task created.", "success")
        return redirect(url_for("tasks_list"))
    return render_template("tasks/form.html", task=None, fields=fields, workers=workers)


@app.route("/tasks/<int:tid>/edit", methods=["GET", "POST"])
@login_required
def tasks_edit(tid):
    with db_conn() as conn:
        task = dict(conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone())
        if g.user["role"] == "worker" and task.get("assigned_user_id") != g.user["id"]:
            flash("You can only edit your own tasks.", "danger")
            return redirect(url_for("tasks_list"))
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
        workers = [dict(u) for u in conn.execute("SELECT * FROM users WHERE role IN ('worker','manager') AND is_active=1 ORDER BY name").fetchall()]
        if request.method == "POST":
            conn.execute("""UPDATE tasks SET field_id=?,assigned_user_id=?,task_type=?,description=?,
                date=?,hours=?,cost=?,status=? WHERE id=?""", (
                int(request.form["field_id"]) if request.form.get("field_id") else None,
                int(request.form["assigned_user_id"]) if request.form.get("assigned_user_id") else None,
                request.form.get("task_type",""), request.form.get("description",""),
                request.form["date"], float(request.form.get("hours") or 0),
                float(request.form.get("cost") or 0), request.form.get("status","pending"), tid,
            ))
            flash("Task updated.", "success")
            return redirect(url_for("tasks_list"))
    return render_template("tasks/form.html", task=task, fields=fields, workers=workers)


@app.route("/tasks/<int:tid>/delete", methods=["POST"])
@login_required
def tasks_delete(tid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("tasks_list"))
    with db_conn() as conn:
        conn.execute("DELETE FROM tasks WHERE id=?", (tid,))
    flash("Task deleted.", "success")
    return redirect(url_for("tasks_list"))


# ── BEEHIVES ──────────────────────────────────────────────────────────────────

@app.route("/beehives")
@login_required
def beehives_list():
    with db_conn() as conn:
        hives = [dict(r) for r in conn.execute("""SELECT b.*,f.name as field_name FROM beehives b
            LEFT JOIN fields f ON b.location_field_id=f.id ORDER BY b.hive_code""").fetchall()]
        for h in hives:
            total = conn.execute("SELECT COALESCE(SUM(qty_liters),0) FROM honey_harvests WHERE hive_id=?",
                                 (h["id"],)).fetchone()[0]
            h["total_honey"] = round(float(total), 1)
            h["honey_logs"] = [dict(r) for r in conn.execute(
                "SELECT * FROM honey_harvests WHERE hive_id=? ORDER BY date DESC LIMIT 5", (h["id"],)).fetchall()]
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
    return render_template("beehives/index.html", hives=hives, fields=fields)


@app.route("/beehives/new", methods=["GET", "POST"])
@login_required
def beehives_new():
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("beehives_list"))
    with db_conn() as conn:
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
    if request.method == "POST":
        with db_conn() as conn:
            conn.execute("""INSERT INTO beehives (hive_code,location_field_id,queen_date,last_inspection_date,health_status,is_producing,notes)
                VALUES (?,?,?,?,?,?,?)""", (
                request.form["hive_code"].strip(),
                int(request.form["location_field_id"]) if request.form.get("location_field_id") else None,
                request.form.get("queen_date") or None,
                request.form.get("last_inspection_date") or None,
                request.form.get("health_status","healthy"),
                1 if request.form.get("is_producing") else 0,
                request.form.get("notes",""),
            ))
        flash("Hive added.", "success")
        return redirect(url_for("beehives_list"))
    return render_template("beehives/form.html", hive=None, fields=fields)


@app.route("/beehives/<int:hid>/edit", methods=["GET", "POST"])
@login_required
def beehives_edit(hid):
    if g.user["role"] not in ("admin", "manager"):
        flash("Manager access required.", "danger")
        return redirect(url_for("beehives_list"))
    with db_conn() as conn:
        hive = dict(conn.execute("SELECT * FROM beehives WHERE id=?", (hid,)).fetchone())
        fields = [dict(f) for f in conn.execute("SELECT * FROM fields ORDER BY name").fetchall()]
        if request.method == "POST":
            conn.execute("""UPDATE beehives SET location_field_id=?,queen_date=?,last_inspection_date=?,
                health_status=?,is_producing=?,notes=? WHERE id=?""", (
                int(request.form["location_field_id"]) if request.form.get("location_field_id") else None,
                request.form.get("queen_date") or None,
                request.form.get("last_inspection_date") or None,
                request.form.get("health_status","healthy"),
                1 if request.form.get("is_producing") else 0,
                request.form.get("notes",""), hid,
            ))
            flash("Hive updated.", "success")
            return redirect(url_for("beehives_list"))
    return render_template("beehives/form.html", hive=hive, fields=fields)


@app.route("/beehives/<int:hid>/honey", methods=["POST"])
@login_required
def beehives_log_honey(hid):
    with db_conn() as conn:
        conn.execute("INSERT INTO honey_harvests (hive_id,date,qty_liters,quality,notes) VALUES (?,?,?,?,?)", (
            hid, request.form["date"], float(request.form["qty_liters"]),
            request.form.get("quality",""), request.form.get("notes",""),
        ))
        conn.execute("UPDATE beehives SET last_inspection_date=? WHERE id=?", (request.form["date"], hid))
    flash("Honey harvest logged.", "success")
    return redirect(url_for("beehives_list"))


# ── REPORTS ───────────────────────────────────────────────────────────────────

@app.route("/reports")
@login_required
def reports_index():
    with db_conn() as conn:
        total_income = float(conn.execute("SELECT COALESCE(SUM(total),0) FROM sales").fetchone()[0])
        total_expenses = float(conn.execute("SELECT COALESCE(SUM(amount),0) FROM expenses").fetchone()[0])
        sales_by_crop = [dict(r) for r in conn.execute("""
            SELECT c.display_name as crop, COALESCE(SUM(s.total),0) as total
            FROM crops c LEFT JOIN sales s ON s.crop_id=c.id
            GROUP BY c.id ORDER BY total DESC""").fetchall()]
    return render_template("reports/index.html",
                           total_income=round(total_income, 2),
                           total_expenses=round(total_expenses, 2),
                           profit=round(total_income - total_expenses, 2),
                           sales_by_crop=sales_by_crop)


@app.route("/reports/backup")
@login_required
def reports_backup():
    if g.user["role"] != "admin":
        flash("Admin only.", "danger")
        return redirect(url_for("reports_index"))
    from db import DB_PATH
    fname = f"farmtracker_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        return send_file(DB_PATH, as_attachment=True, download_name=fname, mimetype="application/octet-stream")
    except Exception as e:
        flash(f"Backup failed: {e}", "danger")
        return redirect(url_for("reports_index"))


# ── REST API ──────────────────────────────────────────────────────────────────

_API_TOKENS: dict = {}  # token -> user_id


def api_auth():
    token = request.args.get("token") or request.headers.get("Authorization", "").replace("Bearer ", "")
    uid = _API_TOKENS.get(token)
    if not uid:
        return None
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=? AND is_active=1", (uid,)).fetchone()
    return dict(row) if row else None


@app.route("/api/token", methods=["POST"])
def api_token():
    data = request.get_json() or {}
    email = data.get("email", "").lower()
    pw = data.get("password", "")
    with db_conn() as conn:
        user = conn.execute("SELECT * FROM users WHERE email=? AND is_active=1", (email,)).fetchone()
    if not user or not verify_password(pw, user["password_hash"]):
        return jsonify({"error": "Invalid credentials"}), 401
    token = secrets.token_hex(32)
    _API_TOKENS[token] = user["id"]
    return jsonify({"token": token, "role": user["role"], "name": user["name"]})


@app.route("/api/dashboard/kpis")
def api_kpis():
    if not api_auth():
        return jsonify({"error": "Unauthorized"}), 401
    today = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    with db_conn() as conn:
        today_s = float(conn.execute("SELECT COALESCE(SUM(total),0) FROM sales WHERE date=?", (today,)).fetchone()[0])
        month_s = float(conn.execute("SELECT COALESCE(SUM(total),0) FROM sales WHERE date>=?", (month_start,)).fetchone()[0])
        stock_v = float(conn.execute("SELECT COALESCE(SUM(qty_on_hand*cost_per_unit),0) FROM inventory_items").fetchone()[0])
        low_s = conn.execute("SELECT COUNT(*) FROM inventory_items WHERE qty_on_hand<=reorder_threshold").fetchone()[0]
        active_f = conn.execute("SELECT COUNT(*) FROM fields WHERE status IN ('planted','growing')").fetchone()[0]
        pending_t = conn.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('pending','in_progress')").fetchone()[0]
        prod_h = conn.execute("SELECT COUNT(*) FROM beehives WHERE is_producing=1").fetchone()[0]
    return jsonify({"today_sales": round(today_s,2), "month_sales": round(month_s,2),
                    "stock_value": round(stock_v,2), "low_stock_count": low_s,
                    "active_fields": active_f, "pending_tasks": pending_t, "producing_hives": prod_h})


@app.route("/api/crops", methods=["GET", "POST"])
def api_crops():
    user = api_auth()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    with db_conn() as conn:
        if request.method == "POST":
            data = request.get_json() or {}
            cur = conn.execute("INSERT INTO crops (slug,display_name,category,notes) VALUES (?,?,?,?)",
                (data["slug"], data["display_name"], data.get("category",""), data.get("notes","")))
            row = conn.execute("SELECT * FROM crops WHERE id=?", (cur.lastrowid,)).fetchone()
            return jsonify(dict(row)), 201
        rows = conn.execute("SELECT * FROM crops ORDER BY display_name").fetchall()
        return jsonify([dict(r) for r in rows])


@app.route("/api/crops/<int:cid>", methods=["GET", "PUT", "DELETE"])
def api_crop(cid):
    user = api_auth()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    with db_conn() as conn:
        if request.method == "GET":
            row = conn.execute("SELECT * FROM crops WHERE id=?", (cid,)).fetchone()
            return jsonify(dict(row)) if row else (jsonify({"error": "Not found"}), 404)
        elif request.method == "PUT":
            data = request.get_json() or {}
            conn.execute("UPDATE crops SET display_name=?,category=?,notes=? WHERE id=?",
                (data.get("display_name"), data.get("category",""), data.get("notes",""), cid))
            row = conn.execute("SELECT * FROM crops WHERE id=?", (cid,)).fetchone()
            return jsonify(dict(row))
        elif request.method == "DELETE":
            conn.execute("DELETE FROM crops WHERE id=?", (cid,))
            return jsonify({"deleted": cid})


@app.route("/api/harvests", methods=["GET", "POST"])
def api_harvests():
    user = api_auth()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    with db_conn() as conn:
        if request.method == "POST":
            data = request.get_json() or {}
            cur = conn.execute("""INSERT INTO harvests (field_id,crop_id,date,qty,unit,quality,storage_location)
                VALUES (?,?,?,?,?,?,?)""",
                (data["field_id"], data["crop_id"], data["date"], data["qty"],
                 data.get("unit","kg"), data.get("quality",""), data.get("storage_location","")))
            row = conn.execute("SELECT * FROM harvests WHERE id=?", (cur.lastrowid,)).fetchone()
            return jsonify(dict(row)), 201
        crop_id = request.args.get("crop_id")
        sql = "SELECT * FROM harvests"
        params = []
        if crop_id:
            sql += " WHERE crop_id=?"; params.append(int(crop_id))
        sql += " ORDER BY date DESC LIMIT 100"
        return jsonify([dict(r) for r in conn.execute(sql, params).fetchall()])


@app.route("/api/sales", methods=["GET", "POST"])
def api_sales():
    user = api_auth()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    with db_conn() as conn:
        if request.method == "POST":
            data = request.get_json() or {}
            qty = float(data["qty"]); price = float(data["price_per_unit"])
            cur = conn.execute("""INSERT INTO sales (crop_id,buyer,qty,unit,price_per_unit,total,date,payment_status)
                VALUES (?,?,?,?,?,?,?,?)""",
                (data["crop_id"], data.get("buyer",""), qty, data.get("unit","kg"),
                 price, round(qty*price,2), data["date"], data.get("payment_status","pending")))
            sid = cur.lastrowid
            inv = f"INV-{data['date'].replace('-','')}-{sid:04d}"
            conn.execute("UPDATE sales SET invoice_id=? WHERE id=?", (inv, sid))
            row = conn.execute("SELECT * FROM sales WHERE id=?", (sid,)).fetchone()
            return jsonify(dict(row)), 201
        return jsonify([dict(r) for r in conn.execute("SELECT * FROM sales ORDER BY date DESC LIMIT 100").fetchall()])


@app.route("/api/inventory", methods=["GET"])
def api_inventory():
    user = api_auth()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    with db_conn() as conn:
        return jsonify([dict(r) for r in conn.execute("SELECT * FROM inventory_items ORDER BY name").fetchall()])


@app.route("/api/inventory/<int:iid>", methods=["PUT"])
def api_inventory_item(iid):
    user = api_auth()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    with db_conn() as conn:
        for field in ["name","category","unit","qty_on_hand","reorder_threshold","cost_per_unit"]:
            if field in data:
                conn.execute(f"UPDATE inventory_items SET {field}=? WHERE id=?", (data[field], iid))
        row = conn.execute("SELECT * FROM inventory_items WHERE id=?", (iid,)).fetchone()
        return jsonify(dict(row)) if row else (jsonify({"error":"Not found"}), 404)


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=5000)
