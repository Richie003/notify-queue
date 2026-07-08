"""Seed sample data by calling the running API (so it exercises the exact
same validation / idempotency / priority code paths a real client would).

Usage:
    python seed.py                     # against http://localhost:8000
    python seed.py --base-url http://localhost:8000
"""
import argparse
import sys

import requests


def seed(base_url: str) -> None:
    session = requests.Session()

    try:
        resp = session.get(f"{base_url}/health/", timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"Could not reach API at {base_url}: {exc}", file=sys.stderr)
        print("Start it first, e.g.: docker-compose up -d  (or) python manage.py runserver", file=sys.stderr)
        sys.exit(1)

    jobs = [
        # (recipient, channel, payload, priority, delay_seconds, idempotency_key)
        ("alice@example.com", "email", {"subject": "Welcome!", "body": "Thanks for signing up."}, "high", 0, "seed-welcome-alice"),
        ("bob@example.com", "email", {"subject": "Weekly digest", "body": "Here's what's new."}, "low", 0, None),
        ("+15551230001", "sms", {"body": "Your OTP is 483920"}, "critical", 0, "seed-otp-bob"),
        ("carol@example.com", "email", {"subject": "Invoice due", "body": "Your invoice is due tomorrow."}, "normal", 5, None),
        ("dave@example.com", "push", {"title": "New message", "body": "You have a new message from Eve"}, "normal", 10, None),
        ("erin@example.com", "email", {"subject": "Password reset", "body": "Click to reset your password."}, "high", 0, None),
        ("+15551230002", "sms", {"body": "Your package has shipped"}, "low", 20, None),
        ("frank@example.com", "email", {"subject": "Newsletter", "body": "Monthly newsletter content."}, "low", 30, None),
        # scheduled well in the future -- should show as 'pending' and NOT be claimed yet
        ("grace@example.com", "email", {"subject": "Reminder", "body": "Your event is tomorrow."}, "normal", 3600, None),
        # deliberately reuses the same idempotency key as the first job above,
        # to demonstrate that it returns the original job instead of creating a new one
        ("alice@example.com", "email", {"subject": "Welcome!", "body": "Thanks for signing up."}, "high", 0, "seed-welcome-alice"),
        # a burst of jobs to the SAME recipient, to demonstrate per-recipient rate limiting
        *[
            ("rate.limited@example.com", "email", {"subject": f"Promo #{i}", "body": "Limited time offer!"}, "normal", 0, None)
            for i in range(1, 9)
        ],
    ]

    created, duplicates = 0, 0
    for recipient, channel, payload, priority, delay, idem_key in jobs:
        body = {
            "recipient": recipient,
            "channel": channel,
            "payload": payload,
            "priority": priority,
            "delay_seconds": delay,
        }
        if idem_key:
            body["idempotency_key"] = idem_key

        resp = session.post(f"{base_url}/jobs/", json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        job = data["job"]
        tag = "DUPLICATE (idempotent, no new job created)" if data["duplicate"] else "created"
        duplicates += data["duplicate"]
        created += not data["duplicate"]
        print(f"[{tag}] {priority:<8} {channel:<5} -> {recipient:<24} id={job['id']}")

    print(f"\nDone. {created} jobs created, {duplicates} recognized as duplicates via idempotency key.")
    print(f"Check progress with: curl {base_url}/metrics/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    args = parser.parse_args()
    seed(args.base_url)
