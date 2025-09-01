# certifyflow-64-66

## Airflow Integration
The certification_backend can integrate with a real Airflow REST API for executing long-running jobs (e2e, performance, soak). Configure via environment variables (do not hardcode):

- AIRFLOW_API_URL: Base URL to Airflow stable API (e.g., https://airflow.example.com/api/v1)
- AIRFLOW_AUTH_SCHEME: one of basic | bearer | none (default: none)
- AIRFLOW_USERNAME: for basic auth
- AIRFLOW_PASSWORD: for basic auth
- AIRFLOW_BEARER_TOKEN: for bearer auth
- AIRFLOW_VERIFY_SSL: "true" | "false" (default: true)
- AIRFLOW_TIMEOUT_SECONDS: single request timeout (default: 15)
- AIRFLOW_POLL_INTERVAL_SECONDS: DAG status poll interval (default: 5)
- AIRFLOW_POLL_TIMEOUT_SECONDS: max wait for job completion (default: 7200)

DAGs expected: cert_e2e, cert_performance, cert_soak
Each DAG should accept a conf payload with "attempt_id".