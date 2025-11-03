import os, time, json, requests, schedule
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY")
DB_URL = os.getenv("DB_URL")

AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

def get_new_requests():
    """Fetch new web-form submissions from the last 15 minutes."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, customer_name, email, subject, description, priority
        FROM support_requests
        WHERE created_at >= NOW() - INTERVAL '15 minutes'
          AND jira_ticket_id IS NULL;
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def create_jira_issue(request):
    """1️⃣ Create a Jira issue."""
    payload = {
        "fields": {
            "project": {"key": PROJECT_KEY},
            "summary": request["subject"],
            "description": f"{request['description']}\n\nCustomer: {request['customer_name']} ({request['email']})",
            "issuetype": {"name": "Service Request"},
            "priority": {"name": request.get("priority", "Medium")},
        }
    }
    resp = requests.post(f"{JIRA_BASE_URL}/rest/api/3/issue", 
                         headers=HEADERS, auth=AUTH, json=payload)
    resp.raise_for_status()
    return resp.json()["key"]

def attach_form(issue_key, form_data):
    """2️⃣ Attach a form file or structured JSON to the issue."""
    # Example: write JSON form locally (could also upload a PDF or CSV)
    filename = f"/tmp/{issue_key}_form.json"
    with open(filename, "w") as f:
        json.dump(form_data, f, indent=2)
    with open(filename, "rb") as f:
        attach_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/attachments"
        attach_headers = {"X-Atlassian-Token": "no-check"}
        resp = requests.post(attach_url, headers=attach_headers, auth=AUTH, files={"file": f})
        resp.raise_for_status()

def update_form_fields(issue_key, form_data):
    """3️⃣ Optionally update custom fields in Jira form."""
    custom_payload = {
        "fields": {
            "customfield_10042": form_data.get("order_id"),
            "customfield_10043": form_data.get("product_type"),
        }
    }
    requests.put(f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}",
                 headers=HEADERS, auth=AUTH, json=custom_payload)

def mark_as_synced(request_id, issue_key):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("UPDATE support_requests SET jira_ticket_id = %s WHERE id = %s", (issue_key, request_id))
    conn.commit()
    cur.close(); conn.close()

def process_requests():
    new_requests = get_new_requests()
    print(f"[{datetime.now()}] Found {len(new_requests)} new requests.")
    for r in new_requests:
        req = {
            "id": r[0], "customer_name": r[1], "email": r[2],
            "subject": r[3], "description": r[4], "priority": r[5]
        }
        try:
            issue_key = create_jira_issue(req)
            attach_form(issue_key, req)
            update_form_fields(issue_key, req)
            mark_as_synced(req["id"], issue_key)
            print(f"✔ Created Jira issue {issue_key} for request {req['id']}")
        except Exception as e:
            print(f"❌ Error creating issue for request {req['id']}: {e}")

# Run every day at 4 PM
schedule.every().day.at("16:00").do(process_requests)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)
