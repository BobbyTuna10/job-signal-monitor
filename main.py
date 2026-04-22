import json
import os
import re
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import unescape
from pathlib import Path
from typing import Any, Optional

import requests


STATE_PATH = Path("state/job_state.json")
USER_AGENT = "job-signal-monitor/1.0"
TIMEOUT = 20
UTC = timezone.utc
DISPLAY_CAP = 15
MIN_SCORE = 3
DEBUG = os.getenv("DEBUG", "false").lower() == "true"


# -----------------------------
# Configure sources here
# -----------------------------
SOURCES: list[dict[str, str]] = [
    {"type": "greenhouse", "token": "hubspot", "label": "HubSpot"},
    {"type": "greenhouse", "token": "robinhood", "label": "Robinhood"},
    {"type": "greenhouse", "token": "airbnb", "label": "Airbnb"},
    {"type": "greenhouse", "token": "stripe", "label": "Stripe"},
    {"type": "greenhouse", "token": "okta", "label": "Okta"},
    {"type": "greenhouse", "token": "zscaler", "label": "Zscaler"},
    {"type": "greenhouse", "token": "affirm", "label": "Affirm"},
    {"type": "greenhouse", "token": "fivetran", "label": "Fivetran"},
    {"type": "greenhouse", "token": "asana", "label": "Asana"},
    {"type": "greenhouse", "token": "gusto", "label": "Gusto"},
    {"type": "greenhouse", "token": "doordashusa", "label": "DoorDash"},
    {"type": "greenhouse", "token": "dropbox", "label": "Dropbox"},
    {"type": "greenhouse", "token": "reddit", "label": "Reddit"},
    {"type": "greenhouse", "token": "discord", "label": "Discord"},
    {"type": "greenhouse", "token": "databricks", "label": "Databricks"},
    {"type": "greenhouse", "token": "elastic", "label": "Elastic"},
    {"type": "greenhouse", "token": "valtech", "label": "Valtech"},
    {"type": "greenhouse", "token": "dept", "label": "DEPT"},
    {"type": "greenhouse", "token": "neweratech", "label": "New Era Technology"},
    {"type": "greenhouse", "token": "credera", "label": "Credera"},
]


# -----------------------------
# Weighted matching rules
# -----------------------------
HIGH_WEIGHT_STACK = {
    "aem": 4,
    "adobe experience manager": 4,
    "sitecore": 4,
    "cms": 4,
    "content management system": 4,
    "dxp": 4,
    "digital experience platform": 4,
    "content platform": 4,
    "web experience": 4,
}

HIGH_WEIGHT_SENIORITY = {
    "director": 3,
    "senior manager": 3,
    "head": 3,
    "lead": 2,
}

MEDIUM_WEIGHT_PRODUCT = {
    "product strategy": 2,
    "platform strategy": 2,
    "digital platform": 2,
    "product platform": 2,
    "experience platform": 2,
}

MEDIUM_WEIGHT_EXPERIENCE = {
    "martech": 2,
    "personalization": 2,
    "customer experience": 2,
    "digital experience": 2,
    "experience platform": 2,
    "adobe": 2,
}

LOW_WEIGHT_SUPPORT = {
    "transformation": 1,
    "product ops": 1,
    "operating model": 1,
    "governance": 1,
    "roadmap": 1,
    "cross-functional": 1,
}

EXCLUDE_TERMS = [
    "engineer",
    "engineering",
    "architect",
    "developer",
    "sales",
    "account executive",
    "recruiter",
    "intern",
    "temporary",
    "risk",
    "analytics",
    "governance lead",
    "enterprise risk",
    "compliance",
    "audit",
    "security",
    "data governance",
    "treasury",
    "finance",
    "legal",
    "privacy",
    "marketing",
    "product marketing",
    "product design",
    "design",
    "designer",
    "learning",
    "technical programs",
    "technical program",
    "program manager",
    "program management",
    "ads platform",
    "writer",
    "account development",
    "business development",
    "partnerships",
    "partnership",
    "strategic account",
    "account executive",
    "sales",
    "policy",
    "specialist",
    "program",
    "programs",
    "account management",
    "corporate development",
    "transportation",
    "logistics",
    "smb",
    "local markets",
    "new verticals",
    "commerce platform",
]

ATLANTA_TERMS = [
    "atlanta",
    "georgia",
    "smyrna",
    "alpharetta",
]

REMOTE_TERMS = [
    "remote",
    "remote - us",
    "remote us",
    "united states",
    "u.s.",
    "us remote",
    "work from home",
]

ATLANTA_BOOST_TERMS = [
    "atlanta",
    "hybrid atlanta",
    "atlanta, ga",
    "georgia",
]


# -----------------------------
# Models
# -----------------------------
@dataclass
class Job:
    source_type: str
    source_company: str
    job_id: str
    title: str
    location: str
    posted_at: Optional[str]
    department: Optional[str]
    description_snippet: Optional[str]
    score: int = 0
    match_reasons: Optional[list[str]] = None
    first_seen_at: Optional[str] = None

    @property
    def fingerprint(self) -> str:
        return f"{self.source_type}:{self.source_company}:{self.job_id}"


# -----------------------------
# Utilities
# -----------------------------
def now_utc() -> datetime:
    return datetime.now(UTC)


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"last_run_at": None, "jobs_seen": {}}
    with STATE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


def request_json(url: str) -> Any:
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = unescape(value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip().lower()


def trim_text(value: Optional[str], limit: int = 400) -> Optional[str]:
    if not value:
        return None
    value = re.sub(r"\s+", " ", unescape(value)).strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def format_posted_at(posted_at: Optional[str]) -> Optional[str]:
    if not posted_at:
        return None

    dt = parse_datetime(posted_at)
    if not dt:
        return posted_at

    return dt.strftime("%Y-%m-%d %H:%M UTC")


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()

    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def add_reason_once(reasons: list[str], label: str) -> None:
    if label not in reasons:
        reasons.append(label)


def escape_html(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# -----------------------------
# Source collectors
# -----------------------------
def fetch_greenhouse(token: str, label: Optional[str] = None) -> list[Job]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    data = request_json(url)

    jobs: list[Job] = []
    for item in data.get("jobs", []):
        title = item.get("title", "") or ""
        location = ((item.get("location") or {}).get("name")) or ""
        posted_at = item.get("updated_at")

        department = None
        if item.get("departments"):
            department_names = [
                d.get("name", "")
                for d in item.get("departments", [])
                if d.get("name")
            ]
            department = ", ".join(department_names) if department_names else None

        snippet = trim_text(item.get("content"))

        jobs.append(
            Job(
                source_type="greenhouse",
                source_company=label or token,
                job_id=str(item.get("id")),
                title=title,
                location=location,
                posted_at=posted_at,
                department=department,
                description_snippet=snippet,
            )
        )
    return jobs


def fetch_lever(handle: str, label: Optional[str] = None) -> list[Job]:
    url = f"https://api.lever.co/v0/postings/{handle}?mode=json"
    data = request_json(url)

    jobs: list[Job] = []
    for item in data:
        categories = item.get("categories") or {}
        location = categories.get("location") or ""
        workplace = categories.get("workplaceType") or ""
        team = categories.get("team") or ""

        combined_location = ", ".join(part for part in [location, workplace] if part)
        department = ", ".join(part for part in [team] if part) or None

        posted_at = None
        created_at = item.get("createdAt")
        if isinstance(created_at, (int, float)):
            posted_at = datetime.fromtimestamp(created_at / 1000, tz=UTC).isoformat()

        description_plain = item.get("descriptionPlain") or item.get("description") or ""
        snippet = trim_text(description_plain)

        jobs.append(
            Job(
                source_type="lever",
                source_company=label or handle,
                job_id=str(item.get("id")),
                title=item.get("text", "") or "",
                location=combined_location,
                posted_at=posted_at,
                department=department,
                description_snippet=snippet,
            )
        )
    return jobs


def fetch_jobs_for_source(source: dict[str, str]) -> list[Job]:
    source_type = source["type"].strip().lower()

    if source_type == "greenhouse":
        return fetch_greenhouse(source["token"], source.get("label"))

    if source_type == "lever":
        return fetch_lever(source["handle"], source.get("label"))

    raise ValueError(f"Unsupported source type: {source_type}")


# -----------------------------
# Filtering and scoring
# -----------------------------
def title_has_target_signal(title: str) -> bool:
    title_text = normalize_text(title)
    target_terms = [
        "product",
        "platform",
        "digital",
        "experience",
        "content",
        "cms",
        "aem",
        "sitecore",
        "martech",
        "web",
        "strategy",
        "operations",
        "transformation",
    ]
    return any(term in title_text for term in target_terms)

def title_penalty_score(title: str) -> tuple[int, list[str]]:
    title_text = normalize_text(title)
    penalty = 0
    reasons: list[str] = []

    has_strong_target = any(term in title_text for term in [
        "platform",
        "product management",
        "digital experience",
        "web experience",
        "experience platform",
        "cms",
        "aem",
        "sitecore",
        "martech",
        "web",
    ])

    if not has_strong_target:
        if "operations" in title_text:
            penalty -= 1
            reasons.append("Operations penalty")
        if "strategy" in title_text:
            penalty -= 1
            reasons.append("Strategy penalty")

    return penalty, reasons
def location_allowed(location: str) -> bool:
    loc = normalize_text(location)
    if not loc:
        return False

    atlanta_terms = [
        "atlanta",
        "atlanta, ga",
        "georgia",
        "smyrna",
        "alpharetta",
    ]

    remote_us_terms = [
        "remote us",
        "remote - us",
        "remote - usa",
        "remote usa",
        "remote united states",
        "remote - united states",
        "united states - remote",
        "usa - remote",
        "u.s. - remote",
    ]

    us_broad_terms = [
        "united states",
        "usa",
        "u.s.",
    ]

    if any(term in loc for term in atlanta_terms):
        return True

    if "remote" in loc and any(term in loc for term in remote_us_terms + us_broad_terms):
        return True

    # Allow broad U.S. listings like "United States" or "USA"
    # so long as they are not clearly non-U.S.
    if any(term in loc for term in us_broad_terms):
        blocked_non_us_terms = [
            "canada",
            "united kingdom",
            "uk",
            "singapore",
            "philippines",
            "malaysia",
            "australia",
            "japan",
            "brazil",
            "toronto",
            "london",
        ]
        if not any(term in loc for term in blocked_non_us_terms):
            return True

    return False
def title_excluded_by_business_function(title: str) -> bool:
    title_text = normalize_text(title)
    blocked_terms = [
        "account development",
        "business development",
        "partnerships",
        "partnership",
        "strategic account",
        "account executive",
        "sales",
    ]
    return any(term in title_text for term in blocked_terms)
def exclusion_hit(job: Job) -> Optional[str]:
    haystack = normalize_text(
        " ".join(
            [
                job.title or "",
                job.department or "",
                job.description_snippet or "",
            ]
        )
    )

    for term in EXCLUDE_TERMS:
        if term in haystack:
            return term
    return None
# -----------------------------
# Filtering and scoring
# -----------------------------

def title_signal_score(title: str) -> tuple[int, list[str]]:
    title_text = normalize_text(title)
    points = 0
    reasons: list[str] = []

    title_terms = {
        "product": ("Product", 1),
        "platform": ("Platform", 2),
        "digital": ("Digital", 1),
        "experience": ("Digital Experience", 1),
        "content": ("Content", 1),
        "cms": ("CMS", 3),
        "aem": ("AEM", 3),
        "sitecore": ("Sitecore", 3),
        "martech": ("Martech", 2),
        "web": ("Web", 2),
        "strategy": ("Strategy", 1),
        "operations": ("Operations", 1),
        "transformation": ("Transformation", 1),
        "product management": ("Product Management", 2),
        "web experience": ("Web Experience", 3),
        "digital experience": ("Digital Experience", 2),
    }

    for term, (label, score) in title_terms.items():
        if term in title_text:
            points += score
            if label not in reasons:
                reasons.append(label)

    return points, reasons

def title_must_have_relevant_signal(title: str) -> bool:
    title_text = normalize_text(title)

    strong_terms = [
        "product management",
        "digital experience",
        "web experience",
        "experience platform",
        "digital platform",
        "content platform",
        "content systems",
        "cms",
        "aem",
        "sitecore",
        "martech",
    ]

    medium_terms = [
        "product",
        "platform",
        "digital",
        "content",
        "web",
        "experience",
    ]

    strong_hit = any(term in title_text for term in strong_terms)
    medium_hits = sum(1 for term in medium_terms if term in title_text)
    
    return strong_hit or medium_hits >= 1
    
def score_job(job: Job) -> tuple[int, list[str]]:
    haystack = normalize_text(
        " ".join(
            [
                job.title or "",
                job.location or "",
                job.department or "",
                job.description_snippet or "",
            ]
        )
    )

    score = 0
    reasons: list[str] = []

    for term, points in HIGH_WEIGHT_STACK.items():
        if term in haystack:
            score += points
            if term in {"aem", "adobe experience manager"}:
                add_reason_once(reasons, "AEM")
            elif term == "sitecore":
                add_reason_once(reasons, "Sitecore")
            elif term in {"cms", "content management system"}:
                add_reason_once(reasons, "CMS")
            else:
                add_reason_once(reasons, "DXP")

    for term, points in HIGH_WEIGHT_SENIORITY.items():
        if term in haystack:
            score += points
            if term == "director":
                add_reason_once(reasons, "Director")
            elif term == "senior manager":
                add_reason_once(reasons, "Senior Manager")
            elif term == "head":
                add_reason_once(reasons, "Head")
            elif term == "lead":
                add_reason_once(reasons, "Lead")

    for term, points in MEDIUM_WEIGHT_PRODUCT.items():
        if term in haystack:
            score += points
            if "platform" in term:
                add_reason_once(reasons, "Platform")
            elif "product" in term:
                add_reason_once(reasons, "Product")
            else:
                add_reason_once(reasons, "Product/Platform")

    for term, points in MEDIUM_WEIGHT_EXPERIENCE.items():
        if term in haystack:
            score += points
            if term == "martech":
                add_reason_once(reasons, "Martech")
            elif term in {"digital experience", "experience platform", "customer experience"}:
                add_reason_once(reasons, "Digital Experience")
            elif term == "adobe":
                add_reason_once(reasons, "Adobe")
            else:
                add_reason_once(reasons, "Experience")

    for term, points in LOW_WEIGHT_SUPPORT.items():
        if term in haystack:
            score += points
            if term == "transformation":
                add_reason_once(reasons, "Transformation")
            elif term == "product ops":
                add_reason_once(reasons, "Product Ops")
            elif term == "governance":
                add_reason_once(reasons, "Governance")
            elif term == "roadmap":
                add_reason_once(reasons, "Roadmap")
            else:
                add_reason_once(reasons, "Strategy/Ops")

    location_text = normalize_text(job.location)
    if contains_any(location_text, ATLANTA_BOOST_TERMS):
        score += 1
        add_reason_once(reasons, "Atlanta")
    # Add title-based scoring
    title_points, title_reasons = title_signal_score(job.title)
    score += title_points
    for reason in title_reasons:
        add_reason_once(reasons, reason)

    # Apply penalty for operations/strategy-heavy titles
    penalty_points, penalty_reasons = title_penalty_score(job.title)
    score += penalty_points
    for reason in penalty_reasons:
        add_reason_once(reasons, reason)

    return score, reasons


def rank_key(job: Job) -> tuple:
    posted_dt = parse_datetime(job.posted_at) or datetime(1970, 1, 1, tzinfo=UTC)
    title_text = normalize_text(job.title)

    director_bonus = 1 if "director" in title_text else 0
    senior_manager_bonus = 1 if "senior manager" in title_text else 0
    cms_bonus = 1 if any(
        t in title_text
        for t in ["aem", "sitecore", "cms", "digital experience platform", "dxp"]
    ) else 0
    atlanta_bonus = 1 if contains_any(normalize_text(job.location), ATLANTA_BOOST_TERMS) else 0

    return (
        job.score,
        director_bonus,
        senior_manager_bonus,
        cms_bonus,
        atlanta_bonus,
        posted_dt.timestamp(),
    )


# -----------------------------
# Digest
# -----------------------------
def render_email_html(
    strong_matches: list[Job],
    total_scanned: int,
    source_count: int,
    errors: list[str],
) -> tuple[str, str]:
    extra_count = max(0, len(strong_matches) - DISPLAY_CAP)
    shown_matches = strong_matches[:DISPLAY_CAP]

    subject = f"Job Signal: {len(shown_matches)} strong matches"
    if extra_count > 0:
        subject += f" (+{extra_count} more)"

    if not shown_matches:
        body = f"""
        <html>
          <body>
            <h2>Job Signal</h2>
            <p>No new strong matches found this run.</p>
            <p><strong>Sources checked:</strong> {source_count}</p>
            <p><strong>Total jobs scanned:</strong> {total_scanned}</p>
          </body>
        </html>
        """
        return subject, body

    cards = []
    for job in shown_matches:
        posted_line = ""
        formatted_posted = format_posted_at(job.posted_at)
        if formatted_posted:
            posted_line = f"<div><strong>Posted:</strong> {formatted_posted}</div>"

        match_reason = " + ".join(job.match_reasons or []) if job.match_reasons else "Strong fit"

        cards.append(
            f"""
            <div style="margin-bottom:16px; padding:12px; border:1px solid #ddd; border-radius:8px;">
              <div style="font-size:18px; font-weight:700;">{escape_html(job.source_company)}</div>
              <div style="margin-top:4px;"><strong>{escape_html(job.title)}</strong></div>
              <div style="margin-top:4px;">{escape_html(job.location or "Location not listed")}</div>
              {posted_line}
              <div style="margin-top:4px;"><strong>Match:</strong> {escape_html(match_reason)}</div>
            </div>
            """
        )

    error_block = ""
    if errors:
        items = "".join(f"<li>{escape_html(err)}</li>" for err in errors)
        error_block = f"""
        <hr>
        <div>
          <strong>Source issues</strong>
          <ul>{items}</ul>
        </div>
        """

    more_line = ""
    if extra_count > 0:
        more_line = f"<p><strong>+{extra_count} additional strong matches not shown</strong></p>"

    body = f"""
    <html>
      <body>
        <h2>Job Signal</h2>
        <p><strong>Strong matches:</strong> {len(shown_matches)}</p>
        {more_line}
        <p><strong>Sources checked:</strong> {source_count}</p>
        <p><strong>Total jobs scanned:</strong> {total_scanned}</p>
        <hr>
        {''.join(cards)}
        {error_block}
      </body>
    </html>
    """
    return subject, body


# -----------------------------
# Email
# -----------------------------
def send_email(subject: str, html_body: str) -> None:
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


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    run_started = now_utc().isoformat()
    state = load_state()
    jobs_seen: dict[str, Any] = state.setdefault("jobs_seen", {})

    if not SOURCES:
        subject = "Job Signal: 0 strong matches"
        body = """
        <html>
          <body>
            <h2>Job Signal</h2>
            <p>The monitor ran successfully, but no sources are configured yet.</p>
            <p>Add Greenhouse and Lever sources inside <code>main.py</code> and rerun.</p>
          </body>
        </html>
        """
        if not DEBUG:
            send_email(subject, body)
        else:
            print(subject)
            print(body)
        state["last_run_at"] = run_started
        save_state(state)
        print("No sources configured. Sent informational email.")
        return

    total_scanned = 0
    errors: list[str] = []
    strong_matches: list[Job] = []

    for source in SOURCES:
        try:
            jobs = fetch_jobs_for_source(source)
            total_scanned += len(jobs)

            for job in jobs:
                if DEBUG:
                    print("\n--- NEW JOB ---")
                    print(f"Company: {job.source_company}")
                    print(f"Title: {job.title}")
                    print(f"Location: {job.location}")

                if not location_allowed(job.location):
                    if DEBUG:
                        print("Excluded: location")
                    continue

                excluded_term = exclusion_hit(job)
                if excluded_term:
                    if DEBUG:
                        print(f"Excluded: term '{excluded_term}'")
                    continue

                if title_excluded_by_business_function(job.title):
                    if DEBUG:
                        print("Excluded: business-function title")
                    continue

                if job.fingerprint in jobs_seen:
                    if DEBUG:
                        print("Excluded: already seen")
                    continue

                score, reasons = score_job(job)

                if DEBUG:
                    print(f"Score: {score}")
                    print(f"Reasons: {reasons}")

                if score < MIN_SCORE:
                    if DEBUG:
                        print("Excluded: below threshold")
                        print(f"Near-miss score: {score}")
                        print(f"Near-miss reasons: {reasons}")
                    continue

                if DEBUG:
                    print("Included")

                job.score = score
                job.match_reasons = reasons
                job.first_seen_at = run_started
                strong_matches.append(job)

        except Exception as exc:
            source_name = source.get("label") or source.get("token") or source.get("handle") or "unknown"
            error_message = f"{source_name}: {str(exc)}"
            errors.append(error_message)
            if DEBUG:
                print(f"Source error: {error_message}")

    strong_matches.sort(key=rank_key, reverse=True)

    subject, html_body = render_email_html(
        strong_matches=strong_matches,
        total_scanned=total_scanned,
        source_count=len(SOURCES),
        errors=errors,
    )

    if not DEBUG:
        send_email(subject, html_body)
    else:
        print("\n=== EMAIL PREVIEW ===")
        print(subject)
        print(html_body[:2000])

    if not DEBUG:
        for job in strong_matches:
            jobs_seen[job.fingerprint] = {
                "source_type": job.source_type,
                "source_company": job.source_company,
                "job_id": job.job_id,
                "title": job.title,
                "location": job.location,
                "posted_at": job.posted_at,
                "score": job.score,
                "match_reasons": job.match_reasons,
                "first_seen_at": job.first_seen_at,
                "alerted_at": run_started,
        }

    state["last_run_at"] = run_started
    save_state(state)

    print(f"Run complete at {run_started}")
    print(f"Sources checked: {len(SOURCES)}")
    print(f"Total jobs scanned: {total_scanned}")
    print(f"Strong matches: {len(strong_matches)}")
    print(f"Errors: {len(errors)}")


if __name__ == "__main__":
    main()
