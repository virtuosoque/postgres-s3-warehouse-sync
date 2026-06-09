# Connecting Power BI to the Viamedia Data Lake

This guide shows how to pull one table from the gateway into Power BI Desktop and
put it on a report. The gateway runs DuckDB over the Iceberg lake and exposes a
small HTTP API; Power BI talks to that API — it never touches Postgres or S3
directly.

Worked example uses the table
**`public__lfg_all_account_deals_line_items_daily_4_live`**. Substitute your own.

---

## 0. What you need

| Thing | Where to get it | Example |
|---|---|---|
| **Gateway URL** | The host running the gateway, port **3000** | `http://10.0.1.25:3000` |
| **Namespace** (schema) | Connections tab → **Namespace** column | `viamedia_sp_dev` |
| **Table name** | Query tab → tables list (it's `<schema>__<table>`) | `public__lfg_all_account_deals_line_items_daily_4_live` |
| **Auth** | None, if the gateway has no IdP configured (the default) | — |

> The fully-qualified name you query is `"<namespace>"."<table_name>"`, e.g.
> `"viamedia_sp_dev"."public__lfg_all_account_deals_line_items_daily_4_live"`.

**Quick connectivity check** (from your machine):
```bash
curl http://<host>:3000/health        # -> {"status":"ok"} (or similar)
```
If that hangs, open port 3000 in the EC2 security group to your IP.

---

## Two ways to query

| | Endpoint | Row cap | Types | Best for |
|---|---|---|---|---|
| **A. Quick (JSON)** | `POST /query` | **10,000** | inferred (datetimes come as text) | previews, small/dim tables |
| **B. Full (Parquet)** | `POST /queries` → poll → signed URL | up to **1,000,000** | preserved from Parquet | fact tables, real loads |

Both honor the SQL guard: **SELECT/WITH only**, tables must be schema-qualified,
and a `LIMIT` is auto-applied (100,000 default, 1,000,000 max). To pull more than
that, filter by `created_at` (see [Large tables](#large-tables)).

---

## Method A — Quick load (no install)

Power BI Desktop → **Get Data → Blank query → Advanced Editor**, paste, and edit
the three caps-marked values:

```m
let
    Base = "http://<HOST>:3000",
    Sql  = "SELECT * FROM ""<NAMESPACE>"".""public__lfg_all_account_deals_line_items_daily_4_live"" LIMIT 1000",
    Resp = Json.Document(
        Web.Contents(Base, [
            RelativePath = "query",
            Headers      = [ #"Content-Type" = "application/json" ],
            Content      = Json.FromValue([ sql = Sql, limit = 1000 ])
        ])
    ),
    Data = Table.FromRows(Resp[rows], Resp[columns])
in
    Data
```

When prompted for credentials for `http://<HOST>:3000`, choose **Anonymous**.
Note the doubled quotes `""` — that's how M escapes the `"` around identifiers.

Datetimes/decimals arrive as text/number via JSON; set column types in Power
Query (**Transform → Data Type**) as needed. For correct types automatically, use
Method B.

---

## Method B — Full load via Parquet (recommended, no install)

Same Advanced Editor, no custom connector. This submits the query, polls until
the gateway has written the result Parquet, then loads it directly:

```m
let
    Base = "http://<HOST>:3000",
    Sql  = "SELECT * FROM ""<NAMESPACE>"".""public__lfg_all_account_deals_line_items_daily_4_live""
            WHERE created_at >= DATE '2026-05-01' AND created_at < DATE '2026-06-01'",

    Submit = Json.Document(
        Web.Contents(Base, [
            RelativePath = "queries",
            Headers      = [ #"Content-Type" = "application/json" ],
            Content      = Json.FromValue([ sql = Sql ])
        ])
    ),
    JobId = Submit[job_id],

    Poll = (n as number) as record =>
        let
            // Query=[_=n] changes the URL each iteration so Power Query doesn't
            // serve a cached (PENDING) response and loop until timeout.
            R = Json.Document(Web.Contents(Base, [
                    RelativePath = "queries/" & JobId, Query = [ #"_" = Text.From(n) ] ]))
        in
            if R[status] = "SUCCEEDED" or R[status] = "FAILED" or n >= 600
            then R
            else Function.InvokeAfter(() => @Poll(n + 1), #duration(0, 0, 0, 1)),

    Final = Poll(0),
    Data  =
        if Final[status] = "SUCCEEDED"
        then Parquet.Document(Web.Contents(Final[result_signed_url]))
        else error "Gateway query " & Final[status] & ": " & (try Text.From(Final[error]) otherwise "")
in
    Data
```

Choose **Anonymous** when prompted. Power BI may also ask to approve the signed
S3 URL host (a "dynamic data source" prompt) — allow it. Click **Close & Apply**.

> **Method B needs real AWS S3.** `result_signed_url` is a presigned S3 URL that
> Power BI fetches directly, so it must be reachable from your machine. This works
> against real S3 (no `AWS_ENDPOINT_URL` set on the gateway). If you're running
> against **LocalStack**, the URL points at an internal endpoint Power BI can't
> reach — use **Method A** instead.

---

## Display it on a report

1. After **Close & Apply**, the query appears in the **Data** pane (rename it,
   e.g. `DealsDaily`).
2. Confirm column types in **Model view** (e.g. `created_at` = Date/Time,
   amounts = Decimal). Fix any that imported as Text.
3. Drop a visual on the canvas:
   - **Table/Matrix:** drag the fields you want into **Columns**.
   - **Bar/line chart:** put `created_at` (or a derived Month) on the axis and a
     measure like `Sum(amount)` on values.
4. Add slicers (e.g. a `created_at` date slicer) so consumers can filter.
5. **Refresh** re-runs the query against the lake — current data, no re-modeling.

---

## Large tables

The gateway caps a single result at **1,000,000 rows**. For bigger tables, don't
pull everything — filter, and let day-partitioning do the pruning:

- **Filter by date in SQL** (Method B): `WHERE created_at >= DATE '...' AND created_at < DATE '...'`.
  Because the table is partitioned by `day(created_at)`, the lake reads only the
  matching days.
- **Power BI Incremental Refresh** (best for fact tables): define `RangeStart` /
  `RangeEnd` parameters and filter `created_at` between them. Power BI then loads
  and refreshes one date-window at a time — each window is a small, partition-
  pruned query. This is exactly what day-partitioning was set up for.
- Or pre-aggregate in SQL (`GROUP BY`) so the result fits under the cap.

---

## Reusable function (`scripts/viamedia_gateway.pq`)

`scripts/viamedia_gateway.pq` packages the Method B (parquet) flow as a plain,
reusable M **function** — not a `.mez` custom connector, so there's no SDK build
and no credential setup.

1. **Get Data → Blank query → Advanced Editor**, paste the file's contents, **OK**.
   Then **in the Queries pane, right-click the query → Rename → `ViamediaQuery`**.
   ⚠️ This rename is required: other queries call it by its *pane name*, not by
   the internal `let` variable. Without it you'll get *"The name 'ViamediaQuery'
   wasn't recognized."* (If you'd rather not manage two queries, use the
   self-contained **Method B** block above — it needs no function name.)
2. New blank query → Advanced Editor:
   ```m
   = ViamediaQuery(
       "http://<HOST>:3000",
       "SELECT * FROM ""<NAMESPACE>"".""public__lfg_all_account_deals_line_items_daily_4_live""
        WHERE created_at >= DATE '2026-05-01' AND created_at < DATE '2026-06-01'"
     )
   ```
3. Pick **Anonymous** when prompted. Pass an OIDC token as an optional 3rd argument
   only if your gateway has an IdP configured.

**Testing with PQTest (Power Query SDK):** evaluating the file returns the function
value (no error). To exercise it, add a test query that invokes it, e.g.
`ViamediaQuery("http://<HOST>:3000", "SELECT 1 AS x")`.

> The earlier version of this file was a `[DataSource.Kind]` custom connector that
> used `Extension.CurrentCredential`. That function only resolves inside a built
> `.mez` connector with a credential set — evaluating the raw `.pq` (e.g. via
> PQTest) fails with *"the name 'Extension.CurrentCredential' wasn't recognized."*
> The plain-function form above avoids that entirely.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `schema '<x>' is not allowed` | Query the **namespace**, not `public`. Use `"<namespace>"."<schema>__<table>"`. The connection's namespace is auto-allowed. |
| `table '<t>' must be schema-qualified` | Always prefix with the namespace and double-quote both parts. |
| `only SELECT / WITH allowed` | The gateway is read-only — no DDL/DML. |
| `LIMIT ... exceeds max 1000000` | Add a `WHERE created_at` filter or aggregate; see [Large tables](#large-tables). |
| Credential / privacy prompt loops | Set the level to the base URL, pick **Anonymous**, and allow the S3 URL when asked. |
| Connection refused | Port 3000 not reachable — open it in the EC2 security group. |
| Empty/locked table | A bootstrap may still be committing chunks; data appears progressively as chunks land. |
