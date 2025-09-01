import os
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Optional, Dict, Any

import httpx


@dataclass
class EmailConfig:
    """Configuration for email delivery loaded from environment variables.

    Supported transports:
      - SMTP: Use SMTP_* variables
      - SENDGRID: Use SENDGRID_API_KEY
      - MAILGUN: Use MAILGUN_API_KEY and MAILGUN_DOMAIN

    Environment variables (do not hardcode):
      - EMAIL_FROM: From email address (required for all providers)
      - EMAIL_PROVIDER: one of smtp | sendgrid | mailgun (default: smtp)

    SMTP:
      - SMTP_HOST: SMTP server hostname
      - SMTP_PORT: SMTP server port (default: 587)
      - SMTP_USERNAME: SMTP username (optional if not required)
      - SMTP_PASSWORD: SMTP password (optional if not required)
      - SMTP_STARTTLS: "true" | "false" (default: true)
      - SMTP_SSL: "true" | "false" (default: false). If true, use SMTPS.

    SendGrid:
      - SENDGRID_API_KEY: API key string

    Mailgun:
      - MAILGUN_API_KEY: API key string
      - MAILGUN_DOMAIN: Domain used in Mailgun (e.g., mg.example.com)
      - MAILGUN_BASE_URL: Optional base URL (default: https://api.mailgun.net)
    """
    provider: str
    email_from: str

    # SMTP
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_starttls: bool = True
    smtp_ssl: bool = False

    # SendGrid
    sendgrid_api_key: Optional[str] = None

    # Mailgun
    mailgun_api_key: Optional[str] = None
    mailgun_domain: Optional[str] = None
    mailgun_base_url: str = "https://api.mailgun.net"

    @staticmethod
    def from_env() -> "EmailConfig":
        provider = os.getenv("EMAIL_PROVIDER", "smtp").strip().lower()
        email_from = os.getenv("EMAIL_FROM", "").strip()
        if not email_from:
            raise ValueError("EMAIL_FROM must be set to enable email notifications")

        cfg = EmailConfig(provider=provider, email_from=email_from)

        if provider == "smtp":
            cfg.smtp_host = os.getenv("SMTP_HOST") or None
            cfg.smtp_port = int(os.getenv("SMTP_PORT", "587"))
            cfg.smtp_username = os.getenv("SMTP_USERNAME") or None
            cfg.smtp_password = os.getenv("SMTP_PASSWORD") or None
            cfg.smtp_starttls = os.getenv("SMTP_STARTTLS", "true").lower() != "false"
            cfg.smtp_ssl = os.getenv("SMTP_SSL", "false").lower() == "true"
            if not cfg.smtp_host:
                raise ValueError("SMTP_HOST is required for SMTP email provider")
        elif provider == "sendgrid":
            cfg.sendgrid_api_key = os.getenv("SENDGRID_API_KEY") or None
            if not cfg.sendgrid_api_key:
                raise ValueError("SENDGRID_API_KEY is required for SendGrid provider")
        elif provider == "mailgun":
            cfg.mailgun_api_key = os.getenv("MAILGUN_API_KEY") or None
            cfg.mailgun_domain = os.getenv("MAILGUN_DOMAIN") or None
            cfg.mailgun_base_url = os.getenv("MAILGUN_BASE_URL", cfg.mailgun_base_url)
            if not cfg.mailgun_api_key or not cfg.mailgun_domain:
                raise ValueError("MAILGUN_API_KEY and MAILGUN_DOMAIN are required for Mailgun provider")
        else:
            raise ValueError(f"Unsupported EMAIL_PROVIDER '{provider}'. Use smtp | sendgrid | mailgun.")
        return cfg


class EmailSender:
    """Email sending abstraction supporting SMTP, SendGrid, and Mailgun."""

    def __init__(self, cfg: EmailConfig) -> None:
        self.cfg = cfg

    # PUBLIC_INTERFACE
    def send_email(self, to_email: str, subject: str, body_text: str, body_html: Optional[str] = None) -> None:
        """Send an email with text and optional HTML body to a single recipient."""
        provider = self.cfg.provider
        if provider == "smtp":
            self._send_smtp(to_email, subject, body_text, body_html)
        elif provider == "sendgrid":
            self._send_sendgrid(to_email, subject, body_text, body_html)
        elif provider == "mailgun":
            self._send_mailgun(to_email, subject, body_text, body_html)
        else:
            # Should not happen due to validation
            raise RuntimeError(f"Unsupported email provider: {provider}")

    def _send_smtp(self, to_email: str, subject: str, body_text: str, body_html: Optional[str]) -> None:
        msg = EmailMessage()
        msg["From"] = self.cfg.email_from
        msg["To"] = to_email
        msg["Subject"] = subject
        if body_html:
            msg.set_content(body_text or "")
            msg.add_alternative(body_html, subtype="html")
        else:
            msg.set_content(body_text or "")

        if self.cfg.smtp_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.cfg.smtp_host, self.cfg.smtp_port, context=context) as server:
                if self.cfg.smtp_username and self.cfg.smtp_password:
                    server.login(self.cfg.smtp_username, self.cfg.smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(self.cfg.smtp_host, self.cfg.smtp_port) as server:
                server.ehlo()
                if self.cfg.smtp_starttls:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                if self.cfg.smtp_username and self.cfg.smtp_password:
                    server.login(self.cfg.smtp_username, self.cfg.smtp_password)
                server.send_message(msg)

    def _send_sendgrid(self, to_email: str, subject: str, body_text: str, body_html: Optional[str]) -> None:
        # https://docs.sendgrid.com/api-reference/mail-send/mail-send
        headers = {
            "Authorization": f"Bearer {self.cfg.sendgrid_api_key}",
            "Content-Type": "application/json",
        }
        data: Dict[str, Any] = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": self.cfg.email_from},
            "subject": subject,
            "content": [],
        }
        if body_html:
            data["content"].append({"type": "text/html", "value": body_html})
        if body_text:
            data["content"].append({"type": "text/plain", "value": body_text})
        if not data["content"]:
            data["content"].append({"type": "text/plain", "value": ""})

        with httpx.Client(timeout=10.0) as client:
            resp = client.post("https://api.sendgrid.com/v3/mail/send", headers=headers, json=data)
            if resp.status_code >= 300:
                raise RuntimeError(f"SendGrid error {resp.status_code}: {resp.text}")

    def _send_mailgun(self, to_email: str, subject: str, body_text: str, body_html: Optional[str]) -> None:
        # https://documentation.mailgun.com/en/latest/api-sending.html#sending
        url = f"{self.cfg.mailgun_base_url}/v3/{self.cfg.mailgun_domain}/messages"
        auth = ("api", self.cfg.mailgun_api_key)
        data = {
            "from": self.cfg.email_from,
            "to": [to_email],
            "subject": subject,
            "text": body_text or "",
        }
        files = None
        if body_html:
            data["html"] = body_html
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, auth=auth, data=data, files=files)
            if resp.status_code >= 300:
                raise RuntimeError(f"Mailgun error {resp.status_code}: {resp.text}")


def format_attempt_email_subject(status: str, run_id: str, attempt_id: str) -> str:
    """Build a concise subject for attempt status notification emails."""
    return f"[Certification] Attempt {attempt_id} for Run {run_id}: {status.upper()}"


def format_attempt_email_body(attempt_obj) -> str:
    """Create a plaintext email body from an attempt object."""
    lines = [
        f"Attempt ID: {attempt_obj.attempt_id}",
        f"Run ID: {attempt_obj.run_id}",
        f"Status: {attempt_obj.status}",
        f"Started At: {getattr(attempt_obj, 'started_at', None)}",
        f"Finished At: {getattr(attempt_obj, 'finished_at', None)}",
        f"Message: {attempt_obj.message or ''}",
        "",
        "Assets:",
    ]
    for a in getattr(attempt_obj, "assets", []) or []:
        try:
            # Support both Pydantic model and dict-like
            name = a.name if hasattr(a, "name") else a.get("name")
            path = a.path if hasattr(a, "path") else a.get("path")
            url = getattr(a, "signed_url", None) if hasattr(a, "signed_url") else (a.get("signed_url") if isinstance(a, dict) else None)
            if url:
                lines.append(f" - {name}: {url}")
            else:
                lines.append(f" - {name}: {path}")
        except Exception:
            continue
    return "\n".join([str(x) for x in lines])


def format_attempt_email_body_html(attempt_obj) -> str:
    """Create a minimal HTML email body from an attempt object."""
    def esc(s: Optional[str]) -> str:
        return (str(s) if s is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows = []
    for a in getattr(attempt_obj, "assets", []) or []:
        try:
            name = a.name if hasattr(a, "name") else a.get("name")
            path = a.path if hasattr(a, "path") else a.get("path")
            url = getattr(a, "signed_url", None) if hasattr(a, "signed_url") else (a.get("signed_url") if isinstance(a, dict) else None)
            link = url or path
            rows.append(f'<li><strong>{esc(name)}:</strong> <a href="{esc(link)}">{esc(link)}</a></li>')
        except Exception:
            continue

    return f"""
<html>
  <body>
    <h3>Certification Attempt Status</h3>
    <p><strong>Attempt ID:</strong> {esc(getattr(attempt_obj,'attempt_id',None))}<br/>
       <strong>Run ID:</strong> {esc(getattr(attempt_obj,'run_id',None))}<br/>
       <strong>Status:</strong> {esc(getattr(attempt_obj,'status',None))}<br/>
       <strong>Started At:</strong> {esc(getattr(attempt_obj,'started_at',None))}<br/>
       <strong>Finished At:</strong> {esc(getattr(attempt_obj,'finished_at',None))}<br/>
       <strong>Message:</strong> {esc(getattr(attempt_obj,'message',None))}</p>
    <h4>Assets</h4>
    <ul>
      {"".join(rows)}
    </ul>
  </body>
</html>
""".strip()
