---
created: 2026-03-23T06:53:08.193Z
title: SQL Server dbo-only table whitelist filtering
area: api
files:
  - backend/app/connectors/sqlserver/connector.py:141
  - backend/app/services/schema_service.py:14
  - backend/app/db/models/connection.py:1
  - backend/app/api/v1/schemas/connection.py:1
  - backend/app/api/v1/endpoints/connections.py:39
  - backend/app/api/v1/endpoints/schemas.py:20
  - backend/app/services/connection_service.py:42
  - frontend/src/pages/ConnectionsPage.tsx:39
  - frontend/src/api/connectionApi.ts:1
  - frontend/src/types/api.ts:1
  - frontend/src/hooks/useConnections.ts:1
---

## Problem

When a MS SQL Server database is connected to QueryWise and introspected, it pulls in ALL tables from ALL schemas. The user only wants:
- Tables from the `dbo` schema only (not other schemas)
- Exclude `TS_*` tables automatically
- Exclude `*backup*` and `*bakup*` tables automatically
- Only cache a user-defined whitelist of exact table names (e.g. `dbo.Customers`, `dbo.Orders`)
- Ability to add/remove tables from the whitelist later without re-introspecting the entire DB
- Other connectors (PostgreSQL, BigQuery, Databricks) must be completely unaffected

## Solution

### Database
- Add `allowed_table_names` nullable JSON column to `database_connections` table via Alembic migration
- Stores exact `"schema.table"` strings e.g. `["dbo.Customers", "dbo.Orders"]`
- Empty/null = no whitelist (but auto-exclusion still applies for SQL Server)

### Filter Logic (schema_service.py `introspect_and_cache`)
For SQL Server connections only:
1. Skip all schemas except `dbo`
2. Auto-exclude tables where name starts with `TS_` (case-insensitive)
3. Auto-exclude tables where name contains `backup` or `bakup` (case-insensitive)
4. If `allowed_table_names` is non-empty: further filter to exact whitelist matches only

### New API endpoint
`GET /connections/{id}/available-tables` — calls connector directly (does NOT update cache), returns all dbo tables after auto-exclusion. Used by the frontend "Manage Tables" modal to show what's available to add to whitelist.

### Frontend
- New "Manage Tables" button (table icon) in connection list action bar, next to introspect icon
- Opens `TableManagerModal`:
  - Left column: Available tables (fetched from `/available-tables`, minus already whitelisted)
  - Right column: Whitelisted tables (current `allowed_table_names`)
  - "Add" and "Remove" buttons to move tables between columns
  - "Save Changes" button -> PATCH connection with updated `allowed_table_names`
  - Footer note: "Re-run introspection to apply changes"
- Connection list shows badge indicating filter status: `[dbo only]` or `[N tables]`

### Backward Compatibility
- Existing connections: `allowed_table_names` is null -> no change in behavior
- Only next re-introspect will apply auto-exclusion for existing SQL Server connections
- Non-SQL Server connections: all new logic is gated behind `connector_type == "sqlserver"` check
