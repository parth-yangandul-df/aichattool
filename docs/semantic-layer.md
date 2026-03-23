# QueryWise — Semantic Layer: Business Logic Reference

**Audience:** Technical leads, senior engineers  
**Purpose:** Explains how QueryWise translates natural language into accurate SQL using a curated business knowledge layer — without hardcoding any business logic into application code.

---

## 1. The Problem It Solves

A raw LLM connected directly to a database schema produces unreliable SQL. It does not know:

- That "ECL" means `SUM(ecl_provisions.ecl_lifetime)`, not a string match on a column called "ecl"
- That `stage = 1` means "Performing", `stage = 2` means "SICR", and `stage = 3` means "Credit-Impaired"
- That "coverage ratio" is `SUM(ecl) / NULLIF(SUM(ead), 0)`, not a free-form calculation
- What business rules govern stage migration under IFRS 9

The semantic layer is the structured answer to all four problems. It is a curated knowledge store, maintained by the business, that sits between the user's question and the LLM — giving the model the context it needs to produce correct, domain-specific SQL every time.

---

## 2. The Four Knowledge Types

The semantic layer is built from four distinct types of business metadata. Each is stored in the database, scoped to a specific connection, and retrieved at query time.

### 2.1 Glossary

Maps business terms to their SQL implementation.

| Field | Example |
|---|---|
| Term | `ECL` |
| Definition | Expected Credit Loss — the probability-weighted estimate of credit losses |
| SQL Expression | `SUM(ecl_provisions.ecl_lifetime)` |
| Related Tables | `ecl_provisions`, `exposures` |

**What it does at query time:** When a user asks "show me total ECL by stage", the glossary resolver finds the "ECL" term, and the SQL expression is injected verbatim into the LLM prompt. The model does not guess — it is given the exact formula.

A connection can hold any number of glossary terms. Terms covering regulatory labels (SICR, PD, LGD, EAD), internal naming conventions, and business-defined filters all belong here.

### 2.2 Metric Definitions

Named, reusable KPI formulas.

| Field | Example |
|---|---|
| Display Name | `Coverage Ratio` |
| SQL Expression | `SUM(ecl_provisions.ecl_lifetime) / NULLIF(SUM(exposures.ead), 0)` |
| Aggregation Type | `ratio` |
| Suggested Dimensions | `stage`, `facility_type`, `sector` |

**What it does at query time:** When a user asks "what is the coverage ratio by segment?", the metric resolver finds the Coverage Ratio metric and injects the complete formula and suggested GROUP BY columns into the LLM prompt. The model cannot silently invent a different formula — it is given the one the business has defined.

Metrics differ from glossary terms in that they are explicitly computational: they always represent an aggregation, a ratio, or a derived measure, and they carry dimension suggestions.

### 2.3 Data Dictionary

Maps raw database codes to human-readable labels, attached to specific columns.

| Column | Raw Value | Display Label |
|---|---|---|
| `exposures.stage` | `1` | Stage 1 — Performing |
| `exposures.stage` | `2` | Stage 2 — SICR |
| `exposures.stage` | `3` | Stage 3 — Credit-Impaired |
| `facilities.facility_type` | `mortgage` | Mortgage Loan |
| `facilities.facility_type` | `corporate` | Corporate Credit |

**What it does at query time:** When the query involves the `exposures.stage` column, all dictionary entries for that column are injected into the LLM prompt. The model then knows that a user asking for "performing loans" must filter `stage = 1`, not `stage = 'performing'`.

Dictionary entries require no vector search — they are fetched by exact column match, only for columns already selected for the query context.

### 2.4 Knowledge Documents

Free-form policy documents, business rules, or regulatory guidance — imported as text or HTML, automatically chunked and made searchable.

| Field | Example |
|---|---|
| Title | `IFRS 9 Staging Policy` |
| Content | The full policy document text |
| Chunk Size | ~450 words per chunk, 80-word overlap |

**What it does at query time:** When a user's question is conceptually related to a policy (e.g., "what triggers a stage migration?"), the most semantically similar chunk is retrieved and injected into the LLM prompt as BUSINESS KNOWLEDGE. The model answers from the organisation's actual policy, not from general training data.

Documents are chunked on import. Each chunk gets its own vector embedding. Retrieval is semantic — the chunk most similar in meaning to the user's question is selected.

---

## 3. How Context Is Built at Query Time

Every query goes through a retrieval pipeline before the LLM is called. The pipeline runs in nine steps.

```
User question
      │
      ▼
Step 1   Embed the question
         Convert the question text into a 1536-dimensional vector
         (or 768-dimensional if using Ollama locally)

Step 2   Find relevant tables — hybrid search
         a. Vector search: cosine similarity between the question
            embedding and every table's description embedding
         b. Keyword search: table names matched against extracted
            keywords from the question
         c. Merge scores: embedding 50%, keyword 30%, FK proximity 20%
         d. FK expansion: include tables connected by foreign key
            to the top-scoring tables

Step 3   Resolve glossary terms
         a. Keyword match: question keywords vs. term names
         b. Direct text match: full term name substring in question
         c. Embedding similarity: top 3 closest terms by cosine distance

Step 4   Resolve metric definitions
         a. Name match: metric name or display name substring in question
         b. Embedding similarity: top 3 by cosine distance

Step 5   Retrieve knowledge chunks
         Vector search: cosine similarity between question embedding
         and every knowledge chunk embedding
         Fallback: keyword ILIKE search if embeddings unavailable

Step 6   Retrieve few-shot examples
         Cosine similarity search across validated sample queries
         Returns the 3 closest matching questions + their SQL

Step 7   Expand FK neighbours
         Any table FK-connected to the selected tables but not yet
         included is added (up to 5 extra tables, score 0.1)

Step 8   Resolve data dictionary + FK edges
         Dictionary entries fetched for every column in selected tables
         FK relationships between selected tables extracted

Step 9   Assemble the prompt context
         All retrieved knowledge formatted into a structured text block
         passed to the LLM as part of the system/user prompt
```

The result is a single structured context block containing: schema, column index, FK relationships, glossary terms with SQL expressions, metric formulas, business knowledge excerpts, data dictionary mappings, and example queries.

---

## 4. The Query Pipeline End-to-End

Once the context is built, the query pipeline runs four sequential agents.

```
User: "What is the total ECL by stage for SICR and credit-impaired loans?"
                                    │
                                    ▼
                          ┌─────────────────┐
                          │  Context Builder │  (9-step retrieval, see above)
                          └────────┬────────┘
                                   │  Assembled context
                                   ▼
                          ┌─────────────────┐
                          │  SQL Composer   │  LLM call #1
                          │                 │
                          │  Input:         │
                          │  - System rules │
                          │  - Schema       │
                          │  - Glossary     │
                          │  - Metrics      │
                          │  - Dictionary   │
                          │  - Knowledge    │
                          │  - Examples     │
                          │                 │
                          │  Output:        │
                          │  - SQL          │
                          │  - Explanation  │
                          │  - Confidence   │
                          │  - Tables used  │
                          │  - Assumptions  │
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │  SQL Validator  │  No LLM call — static analysis
                          │                 │
                          │  Checks:        │
                          │  - Starts with  │
                          │    SELECT/WITH  │
                          │  - No DDL/DML   │
                          │  - Tables exist │
                          │    in schema    │
                          └────────┬────────┘
                                   │
                      Valid ───────┴──────── Invalid
                        │                       │
                        │               ┌───────▼────────┐
                        │               │  Error Handler │  LLM call (up to 3 retries)
                        │               │                │
                        │               │  Input:        │
                        │               │  - Failed SQL  │
                        │               │  - Error msg   │
                        │               │  - Schema      │
                        │               │  - Prior tries │
                        │               │                │
                        │               │  Output:       │
                        │               │  - Fixed SQL   │
                        │               └───────┬────────┘
                        │                       │
                        └───────────────────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │  DB Execution   │  Direct connector call
                          │                 │
                          │  - Timeout: 30s │
                          │  - Max: 1000    │
                          │    rows         │
                          │  - Read-only    │
                          │    enforced     │
                          └────────┬────────┘
                                   │
                              DB error ──── Error Handler (up to 3 retries)
                                   │
                                   ▼
                          ┌─────────────────┐
                          │  Interpreter    │  LLM call #2
                          │                 │
                          │  Input:         │
                          │  - Question     │
                          │  - Executed SQL │
                          │  - Up to 20     │
                          │    result rows  │
                          │                 │
                          │  Output:        │
                          │  - Summary      │
                          │  - Highlights   │
                          │  - Follow-ups   │
                          └────────┬────────┘
                                   │
                                   ▼
                             Full response
                          (SQL + results + summary
                           + follow-up questions)
```

**LLM calls per query:** 2 in the normal path (Composer + Interpreter). Up to 5 if error correction is needed (Composer + up to 3 Error Handler retries + Interpreter).

---

## 5. How Embeddings Work

Vector embeddings are the mechanism that makes semantic search possible. Each piece of metadata is converted to a numeric vector that encodes its meaning. At query time, the user's question is converted to the same vector space, and the closest vectors are retrieved.

| What is embedded | Text used for embedding |
|---|---|
| Database table | `"schema.table_name: description"` |
| Database column | `"table.column (datatype): description"` |
| Glossary term | `"term: definition"` |
| Metric definition | `"display_name: description"` |
| Sample query | The natural language question |
| Knowledge chunk | The chunk text itself |

Embeddings are generated in background tasks — they do not block the main request path. Progress is tracked in memory and exposed via a status API. The frontend polls this and shows a progress banner.

**If embeddings are unavailable** (e.g., model not loaded yet), the system falls back to keyword-only matching — queries still work, but with reduced semantic accuracy.

**Provider:** OpenAI `text-embedding-3-small` (1536 dimensions) by default. Ollama `nomic-embed-text` (768 dimensions) when running locally. Dimension mismatches on provider switch are handled automatically at startup — stale embeddings are nulled and regenerated.

---

## 6. Scoring and Table Selection

When multiple tables are candidates, they are ranked by a weighted score:

| Signal | Weight | How it is measured |
|---|---|---|
| Embedding similarity | 50% | Cosine distance between question vector and table description vector |
| Keyword match | 30% | Exact name → 1.0; component match → 0.7; substring → 0.5 |
| FK relationship | 20% | Table is FK-connected to a top-scoring table |
| Glossary boost | +0.2 additive | Table appears in a matched glossary term's related_tables list |

The top N tables are selected (configurable, default from `settings.max_context_tables`). FK-adjacent tables not yet included are added as neighbours (up to 5 extra, score 0.1) to ensure JOIN paths are always complete.

---

## 7. What the LLM Receives

The LLM does not receive a bare question and a raw schema. It receives a fully-assembled structured context block with nine sections:

```
=== DATABASE SCHEMA ===
public.ecl_provisions — ECL provision records (4,200 rows)
  - provision_id       (uuid, PK, NOT NULL)
  - exposure_id        (uuid, NOT NULL) -- FK to exposures
  - ecl_12m            (numeric)        -- 12-month ECL
  - ecl_lifetime       (numeric)        -- Lifetime ECL
  - pd                 (numeric)        -- Probability of Default
  - lgd                (numeric)        -- Loss Given Default
  ...

=== COLUMN NAME INDEX ===
ecl_provisions: provision_id, exposure_id, ecl_12m, ecl_lifetime, pd, lgd, ...

=== RELATIONSHIPS ===
ecl_provisions.exposure_id -> exposures.id

=== BUSINESS GLOSSARY ===
"ECL": Expected Credit Loss — the probability-weighted estimate of credit losses.
  SQL: SUM(ecl_provisions.ecl_lifetime)
  Tables: ecl_provisions, exposures

"SICR": Significant Increase in Credit Risk — triggers Stage 2 classification.
  SQL: exposures.stage = 2
  Tables: exposures

=== METRIC DEFINITIONS ===
Coverage Ratio (coverage_ratio)
  SQL: SUM(ecl_provisions.ecl_lifetime) / NULLIF(SUM(exposures.ead), 0)
  Suggested dimensions: stage, facility_type, sector

=== BUSINESS KNOWLEDGE ===
[Source: "IFRS 9 Staging Policy"]
A financial asset moves from Stage 1 to Stage 2 when there has been a
significant increase in credit risk since initial recognition...

=== DATA DICTIONARY ===
exposures.stage: 1=Stage 1 - Performing, 2=Stage 2 - SICR, 3=Stage 3 - Credit-Impaired

=== EXAMPLE QUERIES ===
Q: What is the total ECL provision by stage?
SQL: SELECT stage, SUM(ecl_provisions.ecl_lifetime) AS total_ecl
     FROM ecl_provisions JOIN exposures ON ...
     GROUP BY stage ORDER BY stage;

=== CONSTRAINTS ===
Dialect: postgresql. Read-only. Use explicit column names. Use LIMIT, not TOP.
```

The LLM is instructed to use the schema verbatim, apply glossary SQL expressions directly, map dictionary values before filtering, and produce only SELECT statements.

---

## 8. Safety Constraints

Read-only enforcement operates at two independent layers:

**Layer 1 — Static SQL analysis** (runs before every execution):  
Blocklist check: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, and connector-specific variants (`EXPORT DATA`, `LOAD DATA` for BigQuery; `COPY INTO`, `OPTIMIZE`, `VACUUM` for Databricks). Query must begin with `SELECT` or `WITH`.

**Layer 2 — Connector-level enforcement**:  
All queries execute inside a read-only transaction at the database driver level. Even if a write statement somehow passed Layer 1, the database would reject it.

**Row limit:** All queries are capped at `max_rows` (default 1000) and `max_query_timeout_seconds` (default 30s), both configurable per connection.

---

## 9. Metadata Management

All four knowledge types are managed through the API (and correspondingly through the UI). They are scoped to a connection — each database connection has its own isolated semantic layer.

| Type | API path | Embedding on write |
|---|---|---|
| Glossary | `POST /api/v1/connections/{id}/glossary` | Yes — immediate |
| Metrics | `POST /api/v1/connections/{id}/metrics` | Yes — immediate |
| Dictionary | `POST /api/v1/columns/{id}/dictionary` | No — exact match only |
| Knowledge | `POST /api/v1/connections/{id}/knowledge` | Yes — per chunk, immediate |

Updates to glossary terms and metrics trigger re-embedding inline in the same request. Knowledge document imports chunk the document and embed each chunk before returning. Dictionary entries require no embedding — they are always retrieved by exact column ID match.

Schema metadata (tables, columns, relationships) is populated by the introspection endpoint (`POST /api/v1/connections/{id}/introspect`), which reads the live database schema and caches it. Embeddings for schema objects are generated in the background after introspection completes.

---

## 10. Data Flow Summary

```
                         ┌──────────────────────────────────────┐
                         │           Business Metadata           │
                         │                                       │
                         │  Glossary terms                       │
                         │  (term → SQL expression)              │
                         │                                       │
                         │  Metric definitions                   │
                         │  (name → aggregation formula)         │
                         │                                       │
                         │  Data dictionary                      │
                         │  (column → code → label)              │
                         │                                       │
                         │  Knowledge documents                  │
                         │  (policy text → searchable chunks)    │
                         │                                       │
                         │  Schema cache                         │
                         │  (tables, columns, FK relationships)  │
                         └──────────────────┬───────────────────┘
                                            │
                             Retrieval at query time
                             (vector + keyword hybrid)
                                            │
                                            ▼
User question ────────────────► Context Builder ──► Assembled context
                                            │
                                            ▼
                                     SQL Composer (LLM)
                                            │
                                     SQL Validator
                                            │
                                    DB Execution
                                            │
                                   Interpreter (LLM)
                                            │
                                            ▼
                               SQL + results + plain-English summary
                                  + suggested follow-up questions
```

---

## 11. Key Design Decisions

**No business logic in application code.** All domain knowledge — formulas, filters, term definitions, value mappings — lives in the metadata store. Adding a new KPI or correcting a term definition requires no code changes, only a metadata update.

**Hybrid retrieval, not pure vector search.** Keyword matching handles exact term references reliably. Vector search handles paraphrases and synonyms. FK expansion ensures JOIN paths are never broken. All three signals combine into a single ranked score.

**Graceful degradation.** If the embedding model is unavailable, the system falls back to keyword-only retrieval. Queries continue to work; accuracy degrades rather than failing entirely.

**Idempotent setup.** The sample database auto-setup runs on every container start and skips any metadata that already exists. Restarting the stack is always safe.

**Audit trail.** Every query execution — question, generated SQL, final SQL, row count, execution time, LLM provider and model, retry count, and the plain-English summary — is persisted to `query_executions`. Nothing is ephemeral.
