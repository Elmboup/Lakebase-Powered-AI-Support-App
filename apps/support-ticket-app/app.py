# --- diagnostics bootstrap: install crash/exception logging before app code ---
import os as _diag_os
import sys as _diag_sys

_diag_dir = _diag_os.path.dirname(_diag_os.path.abspath(__file__))
if _diag_dir not in _diag_sys.path:
    _diag_sys.path.insert(0, _diag_dir)
import app_diagnostics  # noqa: E402

app_diagnostics.install_diagnostics()
# --- end diagnostics bootstrap ---

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql as pg_sql
import os
from datetime import datetime
from databricks import sdk
import html as html_lib

# Page configuration
st.set_page_config(
    page_title="Support Ticket System",
    page_icon="🎫",
    layout="wide"
)

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

def create_ticket(title, created_by):
    """Create a new support ticket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tickets (title, status, created_by)
        VALUES (%s, 'open', %s)
        RETURNING ticket_id
    """, (title, created_by))
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

# UI Components
def inject_custom_css():
    """Inject the app's custom stylesheet."""
    st.markdown(
        """
        <style>
        /* Hero banner */
        .hero {
            background: linear-gradient(120deg, #6d28d9 0%, #2563eb 55%, #0ea5e9 100%);
            border-radius: 16px;
            padding: 26px 30px;
            color: #fff;
            margin-bottom: 8px;
        }
        .hero h1 { color: #fff; font-size: 1.9rem; font-weight: 700; margin: 0; }
        .hero p { color: rgba(255, 255, 255, 0.85); margin: 6px 0 0; font-size: 0.95rem; }

        /* Ticket cards */
        .ticket-card {
            border: 1px solid rgba(128, 128, 140, 0.35);
            border-radius: 12px;
            padding: 14px 18px 12px;
            margin-bottom: 4px;
            transition: box-shadow 0.15s ease, border-color 0.15s ease;
        }
        .ticket-card:hover {
            box-shadow: 0 4px 18px rgba(0, 0, 0, 0.18);
            border-color: rgba(128, 128, 140, 0.6);
        }
        .ticket-top { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
        .ticket-title { font-size: 1.02rem; font-weight: 600; }
        .ticket-pills { display: flex; gap: 8px; align-items: center; white-space: nowrap; }
        .pill {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.02em;
        }
        .pill-open { background: #fee2e2; color: #b91c1c; }
        .pill-in_progress { background: #fef3c7; color: #b45309; }
        .pill-resolved { background: #d1fae5; color: #047857; }
        .pill-muted { background: rgba(128, 128, 140, 0.15); color: inherit; font-weight: 500; }
        .ticket-footer { font-size: 0.78rem; opacity: 0.75; margin-top: 8px; }

        /* Soften Streamlit widgets */
        div[data-testid="stMetric"] {
            background: rgba(128, 128, 140, 0.08);
            border: 1px solid rgba(128, 128, 140, 0.22);
            border-radius: 12px;
            padding: 12px 10px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


STATUS_META = {
    'open': ('Open', '🔴', 'pill-open'),
    'in_progress': ('In Progress', '🟡', 'pill-in_progress'),
    'resolved': ('Resolved', '🟢', 'pill-resolved'),
}


def display_ticket_card(ticket):
    """Render a ticket as a styled card, returning the HTML markup."""
    label, icon, pill_class = STATUS_META.get(
        ticket['status'], (ticket['status'], '⚪', 'pill-muted')
    )
    title = html_lib.escape(str(ticket['title']))
    created_by = html_lib.escape(str(ticket['created_by']))
    return f"""
    <div class="ticket-card">
        <div class="ticket-top">
            <div class="ticket-title">{icon} {title}</div>
            <div class="ticket-pills">
                <span class="pill {pill_class}">{html_lib.escape(label)}</span>
                <span class="pill pill-muted">💬 {ticket['message_count']} messages</span>
            </div>
        </div>
        <div class="ticket-footer">#{ticket['ticket_id']} · Created by {created_by} on {ticket['created_at'].strftime('%Y-%m-%d %H:%M')}</div>
    </div>
    """

# Main application
def main():
    # Initialize database schema and ticket tables
    init_database()

    # One-time import of tickets created before the app existed
    import_result = import_legacy_tickets()
    if import_result and import_result.get('copied'):
        st.success(
            f"📥 Imported {import_result['copied']} existing ticket(s) "
            f"from schema `{import_result['schema']}`."
        )
    elif import_result and import_result.get('blocked'):
        st.warning(import_result['message'])

    inject_custom_css()

    st.markdown(
        """
        <div class="hero">
            <h1>🎫 Support Ticket System</h1>
            <p>Track, discuss, and resolve support tickets — powered by Databricks Apps &amp; Lakebase</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Sidebar for actions
    with st.sidebar:
        st.header("Actions")
        action = st.radio(
            "Choose an action:",
            ["View All Tickets", "Create New Ticket", "View Ticket Details"]
        )
    
    # View All Tickets
    if action == "View All Tickets":
        st.header("All Support Tickets")

        tickets = get_all_tickets()

        if tickets:
            # Summary metrics
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total tickets", len(tickets))
            m2.metric("Open", sum(1 for t in tickets if t['status'] == 'open'))
            m3.metric("In progress", sum(1 for t in tickets if t['status'] == 'in_progress'))
            m4.metric("Resolved", sum(1 for t in tickets if t['status'] == 'resolved'))

            st.markdown("")

            # Filter by status
            status_filter = st.multiselect(
                "Filter by status:",
                options=['open', 'in_progress', 'resolved'],
                default=['open', 'in_progress', 'resolved']
            )

            filtered_tickets = [t for t in tickets if t['status'] in status_filter]

            st.caption(f"Showing {len(filtered_tickets)} of {len(tickets)} tickets")

            # Two-column card grid
            for i in range(0, len(filtered_tickets), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    if i + j < len(filtered_tickets):
                        col.markdown(
                            display_ticket_card(filtered_tickets[i + j]),
                            unsafe_allow_html=True
                        )
        else:
            st.info("🗒️ No tickets found. Head to **Create New Ticket** in the sidebar to get started!")
    
    # Create New Ticket
    elif action == "Create New Ticket":
        st.header("Create New Support Ticket")
        
        with st.form("new_ticket_form"):
            title = st.text_input("Ticket Title*", max_chars=255)
            created_by = st.text_input("Your Email*", max_chars=100)
            initial_message = st.text_area("Initial Message (optional)", max_chars=1000)
            
            submitted = st.form_submit_button("Create Ticket")
            
            if submitted:
                if not title or not created_by:
                    st.error("Please fill in all required fields (marked with *)")
                elif '@' not in created_by:
                    st.error("Please enter a valid email address")
                else:
                    try:
                        ticket_id = create_ticket(title, created_by)
                        
                        if initial_message:
                            add_message(ticket_id, initial_message, created_by)
                        
                        st.success(f"✅ Ticket #{ticket_id} created successfully!")
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error creating ticket: {str(e)}")
    
    # View Ticket Details
    elif action == "View Ticket Details":
        st.header("Ticket Details")
        
        tickets = get_all_tickets()
        
        if tickets:
            ticket_options = {f"#{t['ticket_id']}: {t['title']}": t['ticket_id'] for t in tickets}
            selected_ticket = st.selectbox("Select a ticket:", options=ticket_options.keys())
            
            if selected_ticket:
                ticket_id = ticket_options[selected_ticket]
                
                # Get ticket details
                ticket = next(t for t in tickets if t['ticket_id'] == ticket_id)
                
                # Display ticket info
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Status", ticket['status'])
                with col2:
                    st.metric("Messages", ticket['message_count'])
                
                st.caption(f"Created by {ticket['created_by']} on {ticket['created_at'].strftime('%Y-%m-%d %H:%M')}")
                
                # Update status
                st.subheader("Update Status")
                new_status = st.selectbox(
                    "Change status to:",
                    options=['open', 'in_progress', 'resolved'],
                    index=['open', 'in_progress', 'resolved'].index(ticket['status'])
                )
                
                if st.button("Update Status"):
                    if new_status != ticket['status']:
                        update_ticket_status(ticket_id, new_status)
                        st.success(f"Status updated to: {new_status}")
                        st.rerun()
                    else:
                        st.info("Status unchanged")
                
                st.divider()
                
                # Display messages
                st.subheader("💬 Conversation")
                messages = get_ticket_messages(ticket_id)
                
                for msg in messages:
                    with st.chat_message("user" if msg['author'] == ticket['created_by'] else "assistant"):
                        st.markdown(f"**{msg['author']}**")
                        st.markdown(msg['message_text'])
                        st.caption(msg['created_at'].strftime('%Y-%m-%d %H:%M:%S'))
                
                # Add new message
                st.subheader("Add Message")
                with st.form("add_message_form"):
                    message_text = st.text_area("Your message*", max_chars=1000)
                    author = st.text_input("Your email*", max_chars=100)
                    
                    submitted = st.form_submit_button("Send Message")
                    
                    if submitted:
                        if not message_text or not author:
                            st.error("Please fill in all required fields")
                        elif '@' not in author:
                            st.error("Please enter a valid email address")
                        else:
                            try:
                                add_message(ticket_id, message_text, author)
                                st.success("Message added successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error adding message: {str(e)}")
        else:
            st.info("No tickets available. Create a ticket first!")

if __name__ == "__main__":
    main()
