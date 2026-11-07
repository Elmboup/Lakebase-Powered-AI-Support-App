# Support Ticket System — Flask + Lakebase Postgres

An internal support-ticket app running on [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html), backed by an autoscaling [Lakebase](https://docs.databricks.com/en/lakebase/index.html) Postgres database. Users create tickets, discuss them in a chat-style conversation, and track resolution status — all persisted in Postgres.

## Architecture

```
Flask + Jinja templates  (app.py, templates/)
    ↓  psycopg2 — one fresh connection per operation
Lakebase Postgres       (Databricks)
    ↑  OAuth credential minted per connection via Databricks SDK
Databricks Apps         (git-backed source, SNAPSHOT deployments)
```

- **Authentication** — there is no password environment variable. Every connection mints a short-lived OAuth token (~1h) via `workspace_client.postgres.generate_database_credential(endpoint=PGENDPOINT)` using the Databricks SDK.
- **Isolation** — the app reads and writes only its own schema, `{PGAPPNAME}_schema_{PGUSER}` (dashes stripped), selected by setting `search_path` on each connection.
- **Bootstrapping** — `startup()` runs at boot: `init_database()` creates the schema and tables if missing, and `import_legacy_tickets()` performs a one-time, non-destructive copy of any tickets that existed in another schema (e.g. created via the SQL editor) into the app schema, advancing id sequences accordingly.

## Features

- Create tickets with an optional first message
- Ticket list with summary stats, status filter chips, and title search
- Ticket detail page with status management (open / in progress / resolved)
- Chat-style conversation with colored author avatars and relative timestamps
- Flash notifications, responsive layout, sticky navigation
- Per-app schema isolation and automatic schema/table initialization

## Data model

```sql
CREATE TABLE tickets (
    ticket_id  SERIAL PRIMARY KEY,
    title      TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'open',
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ticket_messages (
    message_id   SERIAL PRIMARY KEY,
    ticket_id    INTEGER NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
    message_text TEXT NOT NULL,
    author       TEXT NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Configuration

The app reads its database connection settings from environment variables, injected automatically by the bound Postgres resource when deployed on Databricks Apps:

- `PGENDPOINT` — Lakebase endpoint resource path (bound from the `support-app-db` app resource)
- `PGHOST` / `PGPORT` — database host and port
- `PGDATABASE` — database name
- `PGUSER` — database role (also used to namespace the app schema)
- `PGSSLMODE` — SSL mode (defaults to `require`)
- `PGAPPNAME` — application name (used to namespace the schema)

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py        # serves on $PORT (default 8000)
```

Local runs need the `PG*` variables above pointing at a reachable Lakebase Postgres instance and valid Databricks credentials for the SDK.

## Deployment

The app is deployed on Databricks Apps from git (branch `main`, path `apps/support-ticket-app`, SNAPSHOT mode):

- `app.yaml` runs `python app.py` and binds the Lakebase resource `support-app-db` to `PGENDPOINT`.
- `manifest.yaml` declares the Postgres resource with `CAN_CONNECT_AND_CREATE` permission, so the app can create its own schema and tables on first run.
- `requirements.txt` pins Flask, `databricks-sdk`, and `psycopg2-binary` against the Apps constraints file.
