# --- diagnostics bootstrap: install crash/exception logging before app code ---
import os as _diag_os
import sys as _diag_sys

_diag_dir = _diag_os.path.dirname(_diag_os.path.abspath(__file__))
if _diag_dir not in _diag_sys.path:
    _diag_sys.path.insert(0, _diag_dir)
import app_diagnostics  # noqa: E402

app_diagnostics.install_diagnostics()
# --- end diagnostics bootstrap ---

import hashlib
import os
import re
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql as pg_sql
from databricks import sdk

from flask import Flask, render_template, request, redirect, url_for, flash

# Database connection setup
workspace_client = sdk.WorkspaceClient()
endpoint = os.getenv("PGENDPOINT", "")


def get_schema_name():
    """Get the schema name in the format {PGAPPNAME}_schema_{PGUSER}."""
    pgappname = os.getenv("PGAPPNAME", "my_app")
    pguser = os.getenv("PGUSER", "").replace('-', '')
    return f"{pgappname}_schema_{pguser}"


# Database connection using environment variables.
#
# The Databricks Apps runtime injects PGHOST / PGPORT / PGDATABASE / PGUSER /
# PGSSLMODE / PGAPPNAME (not LAKEBASE_* variables, and there is no password
# variable). Lakebase authentication uses a short-lived OAuth token minted via
# the Databricks SDK; a fresh token is generated for every new connection
# because tokens expire after roughly one hour.
def get_db_connection():
    """Create database connection using Databricks-injected environment variables"""
    credential = workspace_client.postgres.generate_database_credential(
        endpoint=endpoint
    )
    conn = psycopg2.connect(
        host=os.environ.get('PGHOST'),
        port=os.environ.get('PGPORT', 5432),
        dbname=os.environ.get('PGDATABASE'),
        user=os.environ.get('PGUSER'),
        password=credential.token,
        sslmode=os.environ.get('PGSSLMODE', 'require'),
        cursor_factory=RealDictCursor
    )
    # Route unqualified table names to this app's isolated schema
    with conn.cursor() as cur:
        cur.execute(
            pg_sql.SQL("SET search_path TO {}").format(
                pg_sql.Identifier(get_schema_name())
            )
        )
    return conn


def init_database():
    """Initialize database schema and ticket tables."""
    schema = pg_sql.Identifier(get_schema_name())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        pg_sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema)
    )
    cur.execute(
        pg_sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.tickets (
                ticket_id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(schema)
    )
    cur.execute(
        pg_sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.ticket_messages (
                message_id SERIAL PRIMARY KEY,
                ticket_id INTEGER NOT NULL REFERENCES {}.tickets(ticket_id) ON DELETE CASCADE,
                message_text TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(schema, schema)
    )
    # Migration: tickets created by older app versions lack the priority column
    cur.execute(
        pg_sql.SQL("""
            ALTER TABLE {}.tickets
            ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'medium'
        """).format(schema)
    )
    conn.commit()
    cur.close()
    conn.close()


def import_legacy_tickets():
    """Copy pre-existing tickets (created before this app existed, e.g. via the
    Lakebase SQL editor under a different role/schema) into this app's own
    isolated schema. Non-destructive: source rows are left in place. A no-op
    once the app schema already holds tickets."""
    app_schema = get_schema_name()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Skip if the app schema already has tickets
        cur.execute(
            pg_sql.SQL("SELECT COUNT(*) AS n FROM {}.tickets").format(
                pg_sql.Identifier(app_schema)
            )
        )
        if cur.fetchone()['n'] > 0:
            return None

        # Find schemas that hold a `tickets` table visible to this app's role
        cur.execute("""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = 'tickets' AND table_schema <> %s
            ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END, table_schema
        """, (app_schema,))
        candidates = [r['table_schema'] for r in cur.fetchall()]

        app = pg_sql.Identifier(app_schema)
        for src in candidates:
            src_id = pg_sql.Identifier(src)
            try:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'tickets'
                """, (src,))
                cols = {r['column_name'] for r in cur.fetchall()}
                if not {'ticket_id', 'title', 'status', 'created_by', 'created_at'} <= cols:
                    continue

                cur.execute(pg_sql.SQL("""
                    INSERT INTO {}.tickets (ticket_id, title, status, created_by, created_at)
                    SELECT ticket_id, title, status, created_by, created_at
                    FROM {}.tickets
                """).format(app, src_id))
                copied = cur.rowcount

                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'ticket_messages'
                """, (src,))
                mcols = {r['column_name'] for r in cur.fetchall()}
                if {'ticket_id', 'message_text', 'author', 'created_at'} <= mcols:
                    if 'message_id' in mcols:
                        cur.execute(pg_sql.SQL("""
                            INSERT INTO {}.ticket_messages
                                (message_id, ticket_id, message_text, author, created_at)
                            SELECT message_id, ticket_id, message_text, author, created_at
                            FROM {}.ticket_messages
                        """).format(app, src_id))
                    else:
                        cur.execute(pg_sql.SQL("""
                            INSERT INTO {}.ticket_messages (ticket_id, message_text, author, created_at)
                            SELECT ticket_id, message_text, author, created_at
                            FROM {}.ticket_messages
                        """).format(app, src_id))

                # Advance the id sequences past the imported rows
                cur.execute(
                    pg_sql.SQL("""
                        SELECT setval(pg_get_serial_sequence(%s, 'ticket_id'),
                                      COALESCE((SELECT MAX(ticket_id) FROM {}.tickets), 0) + 1, false)
                    """).format(app), (f"{app_schema}.tickets",)
                )
                cur.execute(
                    pg_sql.SQL("""
                        SELECT setval(pg_get_serial_sequence(%s, 'message_id'),
                                      COALESCE((SELECT MAX(message_id) FROM {}.ticket_messages), 0) + 1, false)
                    """).format(app), (f"{app_schema}.ticket_messages",)
                )

                conn.commit()
                return {'copied': copied, 'schema': src}
            except psycopg2.Error:
                conn.rollback()
                continue

        pguser = os.environ.get('PGUSER', '<app database role>')
        return {'blocked': True, 'message': (
            "No pre-existing tickets could be imported. If you created `tickets` tables "
            "in another schema (e.g. via the SQL editor), grant this app read access "
            "and reload the page:\n\n"
            f"```sql\nGRANT USAGE ON SCHEMA <your_schema> TO \"{pguser}\";\n"
            f"GRANT SELECT ON ALL TABLES IN SCHEMA <your_schema> TO \"{pguser}\";\n```\n\n"
            "The app expects columns: tickets(ticket_id, title, status, created_by, "
            "created_at) and ticket_messages(ticket_id, message_text, author, created_at)."
        )}
    finally:
        cur.close()
        conn.close()

# Database operations
def get_all_tickets():
    """Fetch all tickets with message counts"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.*, COUNT(tm.message_id) as message_count
        FROM tickets t
        LEFT JOIN ticket_messages tm ON t.ticket_id = tm.ticket_id
        GROUP BY t.ticket_id
        ORDER BY t.created_at DESC
    """)
    tickets = cur.fetchall()
    cur.close()
    conn.close()
    return tickets

def get_ticket_messages(ticket_id):
    """Fetch all messages for a specific ticket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM ticket_messages
        WHERE ticket_id = %s
        ORDER BY created_at ASC
    """, (ticket_id,))
    messages = cur.fetchall()
    cur.close()
    conn.close()
    return messages

def create_ticket(title, created_by, priority="medium"):
    """Create a new support ticket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tickets (title, status, created_by, priority)
        VALUES (%s, 'open', %s, %s)
        RETURNING ticket_id
    """, (title, created_by, priority))
    ticket_id = cur.fetchone()['ticket_id']
    conn.commit()
    cur.close()
    conn.close()
    return ticket_id

def add_message(ticket_id, message_text, author):
    """Add a message to an existing ticket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ticket_messages (ticket_id, message_text, author)
        VALUES (%s, %s, %s)
    """, (ticket_id, message_text, author))
    conn.commit()
    cur.close()
    conn.close()

def update_ticket_status(ticket_id, new_status):
    """Update the status of a ticket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tickets
        SET status = %s
        WHERE ticket_id = %s
    """, (new_status, ticket_id))
    conn.commit()
    cur.close()
    conn.close()


def update_ticket_priority(ticket_id, new_priority):
    """Update the priority of a ticket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tickets
        SET priority = %s
        WHERE ticket_id = %s
    """, (new_priority, ticket_id))
    conn.commit()
    cur.close()
    conn.close()


def delete_ticket(ticket_id):
    """Delete a ticket (its messages cascade) and report how many rows went."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM tickets WHERE ticket_id = %s", (ticket_id,))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return deleted

# Flask application
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24).hex())


@app.template_filter("dt")
def format_datetime(value):
    """Render a timestamp as YYYY-MM-DD HH:MM."""
    return value.strftime("%Y-%m-%d %H:%M")


@app.template_filter("status_label")
def format_status_label(value):
    """Humanize a status value like 'in_progress' -> 'In Progress'."""
    return str(value or "").replace("_", " ").title()


@app.template_filter("timeago")
def format_timeago(value):
    """Render a timestamp as a short relative time like '3h ago'."""
    seconds = (datetime.now() - value).total_seconds()
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{int(minutes)} min ago"
    hours = minutes // 60
    if hours < 24:
        return f"{int(hours)}h ago"
    days = hours // 24
    if days < 7:
        return f"{int(days)}d ago"
    return value.strftime("%Y-%m-%d")


PALETTE = ["#4f46e5", "#0ea5e9", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#14b8a6"]


@app.template_filter("author_color")
def author_color(email):
    """Pick a stable avatar color for an email address."""
    h = int(hashlib.md5(str(email).encode()).hexdigest(), 16)
    return PALETTE[h % len(PALETTE)]


@app.template_filter("initial")
def author_initial(email):
    """First character of an email address for the avatar bubble."""
    return (str(email).strip()[:1] or "?").upper()


STATUSES = ("open", "in_progress", "resolved")
PRIORITIES = ("low", "medium", "high", "urgent")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@app.route("/")
def index():
    """Ticket list with stats, status/priority filters, and title search."""
    status = request.args.get("status")
    priority = request.args.get("priority")
    q = (request.args.get("q") or "").strip()
    tickets = get_all_tickets()
    stats = {
        "total": len(tickets),
        "open": sum(1 for t in tickets if t["status"] == "open"),
        "in_progress": sum(1 for t in tickets if t["status"] == "in_progress"),
        "resolved": sum(1 for t in tickets if t["status"] == "resolved"),
        "urgent": sum(1 for t in tickets if t.get("priority") == "urgent"),
        "high": sum(1 for t in tickets if t.get("priority") == "high"),
        "medium": sum(1 for t in tickets if t.get("priority") == "medium"),
        "low": sum(1 for t in tickets if t.get("priority") == "low"),
    }
    if status in STATUSES:
        tickets = [t for t in tickets if t["status"] == status]
    if priority in PRIORITIES:
        tickets = [t for t in tickets if t.get("priority") == priority]
    if q:
        tickets = [t for t in tickets if q.lower() in str(t["title"]).lower()]
    return render_template(
        "index.html", tickets=tickets, stats=stats, current_filter=status,
        current_priority=priority, q=q
    )


@app.route("/tickets/new", methods=["GET", "POST"])
def new_ticket():
    """Create a new support ticket."""
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        created_by = (request.form.get("created_by") or "").strip()
        initial_message = (request.form.get("initial_message") or "").strip()
        priority = request.form.get("priority") or "medium"

        if not title:
            flash("Please give your ticket a short, descriptive title.", "error")
        elif len(title) > 255:
            flash("Titles are limited to 255 characters — try a shorter summary.", "error")
        elif not created_by:
            flash("Please enter your email address so we can follow up.", "error")
        elif not EMAIL_RE.match(created_by):
            flash("That email address doesn't look right (e.g. you@company.com).", "error")
        elif priority not in PRIORITIES:
            flash("Please pick a valid priority.", "error")
        else:
            try:
                ticket_id = create_ticket(title, created_by, priority)
                if initial_message:
                    add_message(ticket_id, initial_message, created_by)
                flash(f"Ticket #{ticket_id} created successfully!", "success")
                return redirect(url_for("ticket_detail", ticket_id=ticket_id))
            except Exception:
                app.logger.exception("Failed to create ticket")
                flash("We couldn't create your ticket just now — please try again.", "error")

    return render_template("new_ticket.html", form=request.form)


@app.route("/tickets/<int:ticket_id>")
def ticket_detail(ticket_id):
    """Show a ticket's details, status form, and conversation."""
    tickets = get_all_tickets()
    ticket = next((t for t in tickets if t["ticket_id"] == ticket_id), None)
    if ticket is None:
        flash("Ticket not found.", "error")
        return redirect(url_for("index"))
    messages = get_ticket_messages(ticket_id)
    return render_template("ticket_detail.html", ticket=ticket, messages=messages)


@app.route("/tickets/<int:ticket_id>/update", methods=["POST"])
def update_ticket(ticket_id):
    """Update a ticket's status and/or priority."""
    new_status = request.form.get("status")
    new_priority = request.form.get("priority")
    try:
        if new_status in STATUSES:
            update_ticket_status(ticket_id, new_status)
        if new_priority in PRIORITIES:
            update_ticket_priority(ticket_id, new_priority)
        flash("Ticket updated.", "success")
    except Exception:
        app.logger.exception("Failed to update ticket %s", ticket_id)
        flash("We couldn't update the ticket just now — please try again.", "error")
    return redirect(url_for("ticket_detail", ticket_id=ticket_id))


@app.route("/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_ticket_message(ticket_id):
    """Add a message to a ticket's conversation."""
    message_text = (request.form.get("message_text") or "").strip()
    author = (request.form.get("author") or "").strip()

    if not message_text:
        flash("Please write a message before sending.", "error")
    elif len(message_text) > 1000:
        flash("Messages are limited to 1,000 characters.", "error")
    elif not author:
        flash("Please enter your email address.", "error")
    elif not EMAIL_RE.match(author):
        flash("That email address doesn't look right (e.g. you@company.com).", "error")
    else:
        try:
            add_message(ticket_id, message_text, author)
            flash("Message added successfully!", "success")
        except Exception:
            app.logger.exception("Failed to add message to ticket %s", ticket_id)
            flash("We couldn't add your message just now — please try again.", "error")

    return redirect(url_for("ticket_detail", ticket_id=ticket_id))


@app.route("/tickets/<int:ticket_id>/delete", methods=["POST"])
def delete_ticket_route(ticket_id):
    """Delete a ticket (and, by cascade, its messages) after user confirmation."""
    try:
        deleted = delete_ticket(ticket_id)
        if deleted:
            flash(f"Ticket #{ticket_id} was deleted.", "success")
        else:
            flash("That ticket no longer exists.", "error")
    except Exception:
        app.logger.exception("Failed to delete ticket %s", ticket_id)
        flash("We couldn't delete the ticket just now — please try again.", "error")
    return redirect(url_for("index"))


# Initialize the database schema and import any pre-existing tickets at startup.
def startup():
    try:
        init_database()
        result = import_legacy_tickets()
        if result and result.get("copied"):
            print(
                f"Imported {result['copied']} existing ticket(s) "
                f"from schema {result['schema']}"
            )
        elif result and result.get("blocked"):
            print(result["message"])
    except Exception as e:
        print(f"Database initialization failed: {e}")


startup()


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("FLASK_RUN_PORT", 8000)))
    app.run(host=host, port=port, debug=False)


