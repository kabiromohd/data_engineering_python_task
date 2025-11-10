#!/usr/bin/env python
# coding: utf-8

import os
import duckdb
import smtplib
import time
import logging
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD") 
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ZEN_QUOTES_URL = os.getenv("ZEN_QUOTES_URL")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT_SSL = os.getenv("SMTP_PORT_SSL")
DB_PATH = os.getenv("DUCKDB_PATH")
LOG_FILE = os.getenv("LOG_FILE_QUOTE")

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),  # 'w' mode overwrites the file
        logging.StreamHandler()
    ]
)

# Usage
logger = logging.getLogger()

# Database connection
def get_connection():
    logger.info("Connecting to the database...")
    return duckdb.connect(DB_PATH)

# Fetch active users fron duckdb database
def get_active_users(frequency="daily"):
    conn = get_connection()
    logger.info(f"Connected to database to fetch active users with {frequency} frequency")

    query = """
        SELECT name, email
        FROM users
        WHERE subscription_status = 'active'
        AND email_frequency = ?
    """

    users = conn.execute(query, [frequency]).fetchall()
    conn.close()
    logger.info(f"Fetched {len(users)} active users with {frequency} frequency")

    return [{"name": n, "email": e} for n, e in users]

# QUOTE FETCHING FUNCTION FROM API
def fetch_quote():
    logger.info("Fetching quote from API...")
    url = ZEN_QUOTES_URL
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            logger.error(f"Failed to fetch quote, status code: {response.status_code}")
            raise Exception(f"Status code {response.status_code}")
        
        data = response.json()
        
        if not isinstance(data, list) or not data[0].get("q"):
            logger.error(f"Malformed response structure")
            raise Exception("Malformed response structure")

        # Validate structure: expect list with at least one dictionary
        if not isinstance(data, list) or len(data) == 0 or not isinstance(data[0], dict):
            logger.error(f"Unexpected data format")
            raise Exception("Unexpected data format")
        
        quote, author = data[0]["q"].strip(), data[0]["a"].strip()
        logger.info(f"Quote fetched successfully: “{quote}” — {author}")
        return quote, author
    except Exception as e:
        logger.error(f"Quote fetch failed: {e}")
        return None, None

# EMAIL SENDING FUNCTION USERS (with retries)
def send_email(to_email, subject, body, retries=3, delay=5):
    for attempt in range(1, retries + 1):
        try:
            msg = MIMEMultipart()
            msg["From"] = SENDER_EMAIL
            msg["To"] = to_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT_SSL) as server:
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                server.send_message(msg)

            logger.info(f"Email sent to {to_email}")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {to_email}: {e}")
            time.sleep(delay)
    logger.error(f"Failed to send email to {to_email} after {retries} retries")
    return False

# EMAIL SENDING FUNCTION ADMIN (with attachments and retries)
def send_email_admin(to_email, subject, body, attachments=None, retries=3, delay=5):
    """
    Sends an email (with optional attachments) to an admin email address.
    Includes retry logic and works with Gmail App Passwords.
    """
    for attempt in range(1, retries + 1):
        try:
            # Build Email
            msg = MIMEMultipart()
            msg["From"] = SENDER_EMAIL
            msg["To"] = to_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            # Attach Files (if provided)
            if attachments:
                for file_path in attachments:
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as f:
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            "Content-Disposition",
                            f"attachment; filename={os.path.basename(file_path)}",
                        )
                        msg.attach(part)
                    else:
                        logger.warning(f"Attachment not found: {file_path}")

            # --- Send Email via Gmail (SSL connection preferred) ---
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT_SSL) as server:
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                server.send_message(msg)

            logger.info(f"Success: Email sent to {to_email}")
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error("Authentication failed. Check Gmail App Password or account settings.")
            break  # no need to retry if credentials are wrong

        except smtplib.SMTPServerDisconnected:
            logger.warning(f"Attempt {attempt}: Connection unexpectedly closed. Retrying...")
            time.sleep(delay)

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {to_email}: {e}")
            time.sleep(delay)

    logger.error(f"Failed to send email to {to_email} after {retries} retries.")
    print(f"Failed to send email to {to_email} after {retries} retries.")
    return False

# MAIN WORKFLOW
def main():
    logger.info("Fetching quote...")
    quote, author = fetch_quote()
    
    if not quote:
        logger.error("Aborting: No quote available today.")
        return

    logger.info(f"Quote fetched: “{quote}” — {author}")
    logger.info("Preparing to send emails to users...")

    USERS = get_active_users(frequency="daily")
    logger.info(f"Total active users to email: {len(USERS)}")

    total_users = len(USERS)
    success_count = 0
    fail_count = 0
    retry_events = []

    for user in USERS:
        personalized_body = (
            f"Hello {user['name']},\n\n"
            f"Today's quote:\n\n“{quote}” by author: {author}\n\n"
            "Stay inspired,\nFrom Your Daily Quote Service MindFuel"
        )
        subject = f"Your Daily Quote from MindFuel"

        success = send_email(user["email"], subject, personalized_body)
        if success:
            success_count += 1
        else:
            fail_count += 1
            retry_events.append(user["email"])

    # Prepare daily summary for admin
    summary = (
        f"Daily Quote Summary - {datetime.now().strftime('%Y-%m-%d')}\n\n"
        f"Total users: {total_users}\n"
        f"Emails sent successfully: {success_count}\n"
        f"Failed deliveries: {fail_count}\n"
        f"Retried users: {', '.join(retry_events) if retry_events else 'None'}\n"
        f"\nQuote: “{quote}” — {author}"
    )

    # Send summary email to admin with log attachment
    send_email_admin(
        ADMIN_EMAIL,
        "Daily Quote Service Summary (with Logs)",
        summary,
        attachments=[LOG_FILE]
    )

    logger.info(f"Daily summary sent to admin {ADMIN_EMAIL}")
    logger.info(f"Daily summary sent to admin {ADMIN_EMAIL}")
    logger.info("Email delivery process completed. Check log for details.")

if __name__ == "__main__":
    main()