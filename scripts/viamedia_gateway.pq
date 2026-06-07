// PowerBI Power Query M connector for the Viamedia DuckDB gateway.
//
// To use:
//   1. Save this file as ViamediaGateway.pq under your PowerBI custom-connector folder.
//   2. In PowerBI Desktop -> Get Data -> Other -> ViamediaGateway.
//   3. Authenticate via the bearer-token prompt (paste your OIDC access token).
//   4. Pass the SQL to query as a parameter.
//
// The connector polls the async /queries endpoint, then downloads the result
// parquet via the signed S3 URL returned in the status response.

section ViamediaGateway;

[DataSource.Kind="ViamediaGateway", Publish="ViamediaGateway.Publish"]
shared ViamediaGateway.Query = (gatewayUrl as text, sql as text) as table =>
    let
        token = Extension.CurrentCredential()[Key],
        headers = [
            #"Authorization" = "Bearer " & token,
            #"Content-Type"  = "application/json"
        ],
        submit = Json.Document(Web.Contents(
            gatewayUrl,
            [
                RelativePath = "queries",
                Headers = headers,
                Content = Json.FromValue([sql = sql])
            ]
        )),
        jobId = submit[job_id],
        poll = (n as number) as record =>
            let
                resp = Json.Document(Web.Contents(
                    gatewayUrl,
                    [
                        RelativePath = "queries/" & jobId,
                        Headers = headers
                    ]
                )),
                status = resp[status]
            in
                if status = "SUCCEEDED" or status = "FAILED" or n = 600
                    then resp
                    else (Function.InvokeAfter(() => poll(n + 1), #duration(0,0,0,1))),
        final = poll(0),
        url = final[result_signed_url],
        parquet = Parquet.Document(Web.Contents(url))
    in
        parquet;

ViamediaGateway = [
    Authentication = [ Key = [ KeyLabel = "Bearer token (OIDC access token)" ] ],
    Label = "Viamedia DuckDB Gateway"
];

ViamediaGateway.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { "Viamedia DuckDB", "Query the data lake via gateway" }
];
