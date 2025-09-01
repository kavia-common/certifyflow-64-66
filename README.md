# certifyflow-64-66

## Metrics and Scoring

The backend parses results from each certification type and records per-stage metrics and an overall score in AttemptStatus.metrics:

- For pytest: parses junit.xml to compute passed, failed, skipped, errors, and score (percent passed ignoring skipped).
- For pylint: extracts the "rated at X/10" score when present (scaled to 0–100). Heuristic fallback counts error/fatal markers.
- For bandit: counts issues (0 issues = score 100, otherwise 0) with JSON fallback if available.
- For Airflow-driven stages (e2e, performance, soak): reads the stage log JSON and sets score 100 if final state=success, else 0.

AttemptStatus.metrics structure:
{
  "stages": {
    "pytest": { "passed": 10, "failed": 2, "skipped": 1, "errors": 0, "score": 83.33 },
    "pylint": { "passed": 1, "failed": 0, "score": 90.0 },
    "bandit": { "passed": 1, "failed": 0, "score": 100.0 },
    "e2e": { "passed": 1, "failed": 0, "score": 100.0 }
  },
  "overall_score": 93.33
}

Notes:
- If a stage does not produce a parsable report, its score may be null.
- overall_score is the mean of available stage scores (ignores missing/null).

Sample AttemptStatus response (excerpt):
{
  "attempt_id": "abc123",
  "run_id": "run1",
  "status": "succeeded",
  "metrics": {
    "stages": {
      "pytest": { "passed": 20, "failed": 0, "skipped": 2, "errors": 0, "score": 100.0 },
      "pylint": { "passed": 1, "failed": 0, "score": 92.5 },
      "bandit": { "passed": 1, "failed": 0, "score": 100.0 }
    },
    "overall_score": 97.5
  }
}

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
The service can send email notifications when attempt status changes (e.g., to PASSED/SUCCEEDED or FAILED). Email notifications are triggered when a `notification.notification_email` is provided in the CreateRun or CreateAttempt payload and email is configured via environment variables.

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