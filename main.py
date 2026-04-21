import json
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


STATE_PATH = Path("state/job_state.json")


def load_state():
    if not STATE_PATH.exists():
        return {"last_run_at": None, "jobs_seen": {}}
    with STATE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def send_email(subject: str, html_body: str):
    smtp_host = get_required_env("SMTP_HOST")
    smtp_port = int(get_required_env("SMTP_PORT"))
    smtp_user = get_required_env("SMTP_USER")
    smtp_pass = get_required_env("SMTP_PASS")
    email_from = get_required_env("EMAIL_FROM")
    email_to = get_required_env("EMAIL_TO")

    recipients = [x.strip() for x in email_to.split(",") if x.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(email_from, recipients, msg.as_string())


def main():
    now = datetime.now(timezone.utc)

    state = load_state()
    previous_run = state.get("last_run_at")

    subject = "Job Signal Monitor Test"
    html_body = f"""
    <html>
      <body>
        <h2>Job Signal Monitor Test</h2>
        <p>This is a successful test run from GitHub Actions.</p>
        <p><strong>Current run (UTC):</strong> {now.isoformat()}</p>
        <p><strong>Previous run:</strong> {previous_run}</p>
        <p>No jobs are being scanned yet. This is just validating state + email.</p>
      </body>
    </html>
    """

    send_email(subject, html_body)

    state["last_run_at"] = now.isoformat()
    save_state(state)

    print("Test email sent successfully.")
    print(f"Updated state/job_state.json with last_run_at = {state['last_run_at']}")


if __name__ == "__main__":
    main()
