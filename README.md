# certifyflow-64-66

## Metrics and Scoring

The backend parses results from each certification family and records per-stage metrics in AttemptStatus.metrics under a generalized structure, enabling forward-compatible tool additions.

Generalized families and default tools:
- code_quality: tool=pylint (extensible: add flake8, ruff, etc. in future)
- security: tool=bandit (extensible: add safety, semgrep, etc. in future)
- functional_test: tool=pytest (extensible: add nose, unittest, robot, etc.)
- airflow-dispatched: e2e, performance, soak

Metrics are nested by generalized type and include per-tool entries when applicable:

AttemptStatus.metrics example:
{
  "stages": {
    "functional_test": { "pytest": { "passed": 10, "failed": 2, "skipped": 1, "errors": 0, "score": 83.33 }, "score": 83.33 },
    "code_quality":   { "pylint": { "passed": 1, "failed": 0, "score": 90.0 }, "score": 90.0 },
    "security":       { "bandit": { "passed": 1, "failed": 0, "score": 100.0 }, "score": 100.0 },
    "e2e":            { "passed": 1, "failed": 0, "score": 100.0 }
  }
}

Notes:
- If a stage does not produce a parsable report, its score may be null.
- Tools are extensible. Additional tools under code_quality/security/functional_test can be added without changing the schema.

Legacy compatibility:
- Requests may still provide legacy certification types ["pylint","bandit","pytest","e2e","performance","soak"].
- The backend normalizes them to generalized selections internally.

## Requesting Certifications

Preferred (generalized) input:
- CreateRunRequest.certification_selections: [{ "type": "code_quality", "tool": "pylint" }, { "type": "functional_test", "tool": "pytest" }]
- CreateAttemptRequest.certification_selections: same structure

Legacy (deprecated but supported):
- CreateRunRequest.legacy_certification_types: ["pylint","pytest"]
- CreateAttemptRequest.legacy_certification_types: ["bandit"]

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

## Email Notifications
The service can send email notifications when attempt status changes. Email notifications are triggered when a `notification.notification_email` is provided and email is configured via environment variables.

Environment variables (do not hardcode):

Common:
- EMAIL_PROVIDER: one of smtp | sendgrid | mailgun (default: smtp)
- EMAIL_FROM: Email address used as the sender (required)

SMTP:
- SMTP_HOST: Server host (required when EMAIL_PROVIDER=smtp)
- SMTP_PORT: Port (default: 587)
- SMTP_USERNAME: Username (optional)
- SMTP_PASSWORD: Password (optional)
- SMTP_STARTTLS: "true" | "false" (default: "true")
- SMTP_SSL: "true" | "false" (default: "false"). If true, SMTPS is used.

SendGrid:
- SENDGRID_API_KEY: API key (required when EMAIL_PROVIDER=sendgrid)

Mailgun:
- MAILGUN_API_KEY: API key (required when EMAIL_PROVIDER=mailgun)
- MAILGUN_DOMAIN: Domain (e.g., mg.example.com) (required)
- MAILGUN_BASE_URL: Optional (default: https://api.mailgun.net)

Usage:
1. Configure environment variables for your provider.
2. Include `notification.notification_email` in the request body for `/runs` or `/runs/{run_id}/attempts`.
3. On status changes (run/attempt completion), the service will send an email with attempt details and asset links (presigned if object storage is configured).