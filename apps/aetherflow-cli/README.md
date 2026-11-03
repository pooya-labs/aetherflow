# AetherFlow CLI

The aetherflow binary is an HTTP client for the versioned AetherFlow control plane. It never opens a database connection or starts workers.

## Configuration

- AETHERFLOW_URL: server base URL; defaults to http://127.0.0.1:8080.
- AETHERFLOW_API_TOKEN: platform/operator Bearer credential.
- AETHERFLOW_TENANT_TOKEN: tenant-bound Bearer credential, selected with --tenant.

Tokens are read from environment variables, never accepted as command-line values, and are not printed. Redirects are disabled so authenticated requests are not forwarded to another origin. HTTPS verification remains enabled using the platform trust configuration. Plain HTTP is accepted only for loopback addresses; remote server URLs must use HTTPS, so the CLI never sends a Bearer token to a remote cleartext endpoint.

## Commands

~~~bash
aetherflow health
aetherflow workers
aetherflow workflow inspect <workflow-run-id>
aetherflow workflow submit --file <request.json>
aetherflow task inspect <task-run-id>
aetherflow task cancel <task-run-id> --expected-version <version>
~~~

Add --tenant to workflow/task commands to use the tenant API and AETHERFLOW_TENANT_TOKEN. Worker inventory is operator-only. Use --url or AETHERFLOW_URL to select a non-default server.

Submission files contain the v1 SubmitWorkflowRequest JSON object described in the OpenAPI contract. Use --file - to read the JSON object from standard input. Standard input and files are read through a 2 MiB bounded reader. IDs are checked locally against the server's ASCII identifier contract before a request is sent.
