"""ONE-OFF diagnostic: sends one test Square Purchase to Meta with a test_event_code, so Meta support can watch how
a Square Purchase is processed (Events Manager > Test events). It is not a feature: never import it from main.py,
never wire it into a route or a cron job.

The test event is the twin of one real, already-sent Square Purchase (--mirror-event-id, an sq_pay_ event in
capi_events_log): same customer, same value, same action_source, user_data from the same helper. Only two things
differ: event_id is sq_pay_test_<unix time at firing> (can never collide with a real sq_pay_<payment_id>) and
event_time is the time of firing. test_event_code sits at the top level of the body, next to "data"; the
production sender never sends one, which is why this is a standalone script.

The payload is built here with the production helpers (build_meta_user_identifiers, browser_user_data,
meta_event_time). The dry run also runs the real production sender, main.fire_meta_capi_event, against a fake
cursor with requests.post captured (nothing is sent, nothing is written), and checks the two payloads are equal
field for field apart from test_event_code.

Read-only database: the session is opened read-only, the script writes nothing to capi_events_log or any other
table, so the test event never appears in reconciliation counts, the thermostat or audience logic.

Run where DATABASE_URL, META_PIXEL_ID and META_ACCESS_TOKEN are set (e.g. `render ssh <service> --ephemeral`):
    python scripts/fire_test_purchase.py --mirror-event-id <sq_pay_...> --test-event-code <code>          # dry run
    python scripts/fire_test_purchase.py --mirror-event-id <sq_pay_...> --test-event-code <code> --fire   # once
The test code may come from META_TEST_EVENT_CODE instead of the command line. Prints hashed identifiers only,
never the access token or raw contact details. Sends at most one request and never retries.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402

GRAPH_URL = "https://graph.facebook.com/v21.0/{pixel}/events"


def load(mirror_event_id: str):
    """The real event (read from capi_events_log) and its customer's stored record. Read-only session."""
    conn = main.get_conn()
    conn.set_session(readonly=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT event_id, cluster_id, event_name, event_value, bowler_value_score, sent_to_meta,
                                  meta_response_status, event_time, sent_at
                           FROM capi_events_log WHERE event_id = %s;""", (mirror_event_id,))
            real = cur.fetchone()
            if not real:
                sys.exit(f"{mirror_event_id}: not in capi_events_log")
            cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (real["cluster_id"],))
            cust = cur.fetchone()
            if not cust:
                sys.exit(f"cluster {real['cluster_id']}: no customer record")
        conn.rollback()
    finally:
        conn.close()
    return dict(real), dict(cust)


def build_event(cust: dict, value: float, event_id: str, fired_at: datetime) -> dict:
    """The event, built exactly the way fire_meta_capi_event builds a Square Purchase."""
    stamped = main.meta_event_time(fired_at, now=fired_at)
    event = {
        "event_name": "Purchase",
        "event_time": int(stamped.timestamp()),
        "event_id": event_id,
        "action_source": "physical_store",
        # browser_user_data is {} for sq_pay_ events (Square POS has no browser): no fbp, fbc, IP or user agent.
        "user_data": {**main.build_meta_user_identifiers(cust), **main.browser_user_data(cust, event_id)},
        "custom_data": {
            "pg_persona": cust.get("pg_mosaic_persona"),
            "bowler_value_score": int(cust.get("bowler_value_score") or 0),
        },
    }
    if value and float(value) > 0:
        event["custom_data"].update(currency="USD", value=round(float(value), 2))
    return event


class _FakeCursor:
    """Stands in for the database while the production sender runs in the dry run: every query finds nothing
    (no prior send, no twin), and every write is dropped."""
    def execute(self, *_a, **_k):
        pass

    def fetchone(self):
        return None

    def fetchall(self):
        return []


def production_payload(cust: dict, value: float, event_id: str, fired_at: datetime) -> dict:
    """What main.fire_meta_capi_event itself would POST for this event, with requests.post captured."""
    captured = {}

    class _Resp:
        status_code, text = 200, "{}"

    def fake_post(url, params=None, json=None, timeout=None):
        captured.update(url=url, json=json)
        return _Resp()

    real_post, main.requests.post = main.requests.post, fake_post
    try:
        main.fire_meta_capi_event(_FakeCursor(), {**cust, "enrich_after": None}, "Purchase", value,
                                  0, event_id=event_id, action_source="physical_store", event_time=fired_at)
    finally:
        main.requests.post = real_post
    if not captured:
        sys.exit("the production sender built no request (META_PIXEL_ID / META_ACCESS_TOKEN unset?)")
    return captured["json"]


def diff(a, b, path="") -> list:
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            out += diff(a.get(k, "<missing>"), b.get(k, "<missing>"), f"{path}.{k}" if path else k)
        return out
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [m for i, (x, y) in enumerate(zip(a, b)) for m in diff(x, y, f"{path}[{i}]")]
    return [] if a == b else [f"{path}: script={a!r} production={b!r}"]


def main_cli():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--mirror-event-id", required=True, help="the real sq_pay_ event to mirror")
    ap.add_argument("--test-event-code", default=os.getenv("META_TEST_EVENT_CODE"),
                    help="Meta's test event code (or META_TEST_EVENT_CODE)")
    ap.add_argument("--fire", action="store_true", help="send it, once (default: dry run, sends nothing)")
    args = ap.parse_args()

    if not args.mirror_event_id.startswith("sq_pay_") or "_test_" in args.mirror_event_id:
        sys.exit("--mirror-event-id must be a real Square Purchase (sq_pay_<payment_id>)")
    if not args.test_event_code:
        sys.exit("no test event code: pass --test-event-code or set META_TEST_EVENT_CODE")
    if not (main.META_PIXEL_ID and main.META_ACCESS_TOKEN and main.DATABASE_URL):
        sys.exit("DATABASE_URL, META_PIXEL_ID and META_ACCESS_TOKEN must be set")

    real, cust = load(args.mirror_event_id)
    if real["event_name"] != "Purchase" or not real["sent_to_meta"]:
        sys.exit(f"{args.mirror_event_id}: not a sent Purchase ({real['event_name']}, {real['meta_response_status']})")
    value = float(real["event_value"])

    fired_at = datetime.now(timezone.utc).replace(microsecond=0)
    event_id = f"sq_pay_test_{int(fired_at.timestamp())}"
    event = build_event(cust, value, event_id, fired_at)
    body = {"data": [event], "test_event_code": args.test_event_code}

    print(f"mirroring {real['event_id']}: cluster {real['cluster_id']}, value {value:.2f}, "
          f"score at send {real['bowler_value_score']}, event_time {real['event_time']}, sent {real['sent_at']}")
    if int(cust.get("bowler_value_score") or 0) != int(real["bowler_value_score"] or 0):
        print(f"NOTE: the customer's score is now {cust.get('bowler_value_score')} "
              f"(was {real['bowler_value_score']} at send); the test event carries the stored record's value")

    mismatches = diff({"data": body["data"]}, production_payload(cust, value, event_id, fired_at))
    if mismatches:
        print("MISMATCH with the production sender:\n  " + "\n  ".join(mismatches))
        sys.exit(1)
    print("field-for-field match with main.fire_meta_capi_event (only test_event_code is added)")
    print(f"\nPOST {GRAPH_URL.format(pixel=main.META_PIXEL_ID)}  (access_token as a query parameter, not shown)")
    print(json.dumps(body, indent=2))   # user_data holds SHA-256 hashes only

    if not args.fire:
        print("\nDRY RUN: nothing sent. Re-run with --fire to send it once.")
        return

    try:
        resp = requests.post(GRAPH_URL.format(pixel=main.META_PIXEL_ID),
                             params={"access_token": main.META_ACCESS_TOKEN}, json=body, timeout=15)
    except requests.RequestException as e:
        print(f"\nREQUEST FAILED ({type(e).__name__}); not retried")
        sys.exit(1)
    print(f"\nHTTP {resp.status_code}")
    print(resp.text.replace(main.META_ACCESS_TOKEN, "<redacted>"))
    try:
        received = resp.json().get("events_received")
    except ValueError:
        received = None
    if resp.status_code != 200 or received != 1:
        print("NOT ACCEPTED as expected (want HTTP 200 with events_received: 1); not retried")
        sys.exit(1)
    print(f"OK: {event_id} accepted under the test event code")


if __name__ == "__main__":
    main_cli()
