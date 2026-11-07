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
def display_ticket_card(ticket):
    """Display a ticket as a card"""
    status_colors = {
        'open': '🔴',
        'in_progress': '🟡',
        'resolved': '🟢'
    }
    
    status_icon = status_colors.get(ticket['status'], '⚪')
    
    with st.container():
        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            st.markdown(f"### {status_icon} {ticket['title']}")
        with col2:
            st.caption(f"Status: **{ticket['status']}**")
        with col3:
            st.caption(f"💬 {ticket['message_count']} messages")
        
        st.caption(f"Created by {ticket['created_by']} on {ticket['created_at'].strftime('%Y-%m-%d %H:%M')}")
        st.divider()

# Main application
def main():
    # Initialize database schema and ticket tables
    init_database()

    st.title("🎫 Support Ticket System")
    st.markdown("*Powered by Databricks Apps & Lakebase*")
    
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
            # Filter by status
            status_filter = st.multiselect(
                "Filter by status:",
                options=['open', 'in_progress', 'resolved'],
                default=['open', 'in_progress', 'resolved']
            )
            
            filtered_tickets = [t for t in tickets if t['status'] in status_filter]
            
            st.caption(f"Showing {len(filtered_tickets)} of {len(tickets)} tickets")
            
            for ticket in filtered_tickets:
                display_ticket_card(ticket)
        else:
            st.info("No tickets found. Create your first ticket!")
    
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
