// Reusable Power Query (M) FUNCTION for the Viamedia DuckDB gateway.
//
// This is a plain function -- NOT a packaged custom connector. That means it
// evaluates directly (PQTest) and works in Power BI's Advanced Editor with no
// SDK build, no .mez, and no credential setup. (The previous version used
// Extension.CurrentCredential, which only resolves inside a built connector with
// a credential context -- hence the "name wasn't recognized" error under PQTest.)
//
// Flow: POST /queries {sql} -> poll GET /queries/{job_id} until SUCCEEDED ->
// load the result parquet from the signed S3 URL. The parquet path preserves
// column types and is not subject to the 10k-row cap of the sync /query endpoint
// (still bounded by the gateway guard: default LIMIT 100000, max 1000000).
//
// USAGE in Power BI Desktop:
//   1. Get Data -> Blank query -> Advanced Editor -> paste this -> OK.
//      Rename the query "ViamediaQuery". (It evaluates to a function.)
//   2. New blank query, Advanced Editor:
//        = ViamediaQuery(
//            "http://<HOST>:3000",
//            "SELECT * FROM ""<NAMESPACE>"".""public__lfg_all_account_deals_line_items_daily_4_live""
//             WHERE created_at >= DATE '2026-05-01' AND created_at < DATE '2026-06-01'"
//          )
//   3. When prompted for credentials on http://<HOST>:3000, choose Anonymous.
//
// `token` is OPTIONAL: omit for an anonymous gateway; pass an OIDC access token
// when GATEWAY_JWKS_URL / GATEWAY_JWT_ISSUER are configured.
//
// TEST with PQTest: evaluating this file returns the function value. To exercise
// it, add a sibling test query that invokes it, e.g.
//   ViamediaQuery("http://<HOST>:3000", "SELECT 1 AS x")

let
    ViamediaQuery = (gatewayUrl as text, sql as text, optional token as nullable text) as table =>
        let
            authHeader =
                if token <> null and token <> ""
                then [ #"Authorization" = "Bearer " & token ]
                else [],
            headers = Record.Combine({ [ #"Content-Type" = "application/json" ], authHeader }),

            submit = Json.Document(Web.Contents(
                gatewayUrl,
                [
                    RelativePath = "queries",
                    Headers = headers,
                    Content = Json.FromValue([ sql = sql ])
                ]
            )),
            jobId = submit[job_id],

            poll = (n as number) as record =>
                let
                    resp = Json.Document(Web.Contents(
                        gatewayUrl,
                        [
                            RelativePath = "queries/" & jobId,
                            Headers = headers,
                            // Changing query param defeats Power Query's response
                            // cache; without it the loop reuses the first PENDING
                            // response forever and times out.
                            Query = [ #"_" = Text.From(n) ]
                        ]
                    )),
                    status = resp[status]
                in
                    if status = "SUCCEEDED" or status = "FAILED" or n >= 600
                    then resp
                    else Function.InvokeAfter(() => @poll(n + 1), #duration(0, 0, 0, 1)),

            final = poll(0),
            result =
                if final[status] = "SUCCEEDED"
                then Parquet.Document(Web.Contents(final[result_signed_url]))
                else error Error.Record(
                    "ViamediaGateway",
                    "Query " & final[status] & ": " & (try Text.From(final[error]) otherwise "")
                )
        in
            result
in
    ViamediaQuery
