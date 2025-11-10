import os, time, json, requests
import psycopg2
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd

load_dotenv()

# Jira configuration from environment variables
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY")
ISSUE_TYPE = os.getenv("ISSUE_TYPE")
LOG_FILE = os.getenv("LOG_FILE_JIRA")
DB_URL = os.getenv("DB_URL")
INITIAL_CSV_PATH = os.getenv("INITIAL_CSV_PATH")
FETCH_FILE_PATH = os.getenv("FETCH_FILE_PATH")
LAST_RUN_FILE_PATH = os.getenv("LAST_RUN_FILE_PATH")
PREPROCESSED_CSV_PATH = os.getenv("PREPROCESSED_CSV_PATH")

# Database connection details
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOSTNAME = os.getenv("DB_HOSTNAME")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")

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

def get_supabase_connection():
    """
    Connects to a Supabase PostgreSQL database using environment variables.
    Returns a psycopg2 connection object.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOSTNAME"),
            port=os.getenv("DB_PORT"),
        )
        logger.info("Successfully connected to Supabase database.")
        return conn

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None


def save_last_run_date(LAST_RUN_FILE_PATH):
    # Format as YYYY-MM-DD
    last_run = {"last_run_date": datetime.now().strftime("%Y-%m-%d")}

    with open(LAST_RUN_FILE_PATH, "w") as f:
        json.dump(last_run, f)
    logger.info(f"Last run date saved: {last_run['last_run_date']}")

def get_last_run_date(LAST_RUN_FILE_PATH):
    try:
        with open(LAST_RUN_FILE_PATH, "r") as f:
            data = json.load(f)
            # Parse back into a datetime.date object
            return datetime.strptime(data["last_run_date"], "%Y-%m-%d").date()
    except FileNotFoundError:
        logger.info("No previous run found. Assuming first run.")
        return None
    
def get_new_requests():
    """Fetch new web-form submissions from the last 15 minutes."""

    conn = get_supabase_connection()
    logger.info("Connected to the database.")
    
    if conn is None:
        logger.error("Failed to connect to the database.")
        return []

    cur = conn.cursor()

    last_run = get_last_run_date(LAST_RUN_FILE_PATH)

    if last_run:
        logger.info(f"Last run was at: {last_run}")
        logger.info("Fetching new requests from the database...")
        cur.execute(f"""
            SELECT *
            FROM {DB_TABLE_NAME}
            WHERE created_at > '{last_run}';
        """)
    else:
        logger.info("This is a first run, fetching all requests from the database...")
        cur.execute(f"""
            SELECT *
            FROM {DB_TABLE_NAME};
        """)

    rows = cur.fetchall()
    cur.close(); conn.close()
    logger.info("Database connection closed.")

    save_last_run_date(LAST_RUN_FILE_PATH)
    logger.info("Last run time saved.")

    # Fetch column names
    columns = [desc[0] for desc in cur.description]
    if not rows:
        logger.info("No new requests found.")
        return [], columns

    logger.info(f"Fetched {len(rows)} new requests.")
    return rows, columns


def preprocess_request(data):
    """Preprocess data from supabase"""

    # Fill missing approximateendingdate with dateneededby
    data["approximateendingdate"] = data["approximateendingdate"].fillna(data["dateneededby"])
    # Save preprocessed data to CSV
    logger.info(f"Saving preprocessed data to {PREPROCESSED_CSV_PATH}")
    data.to_csv(PREPROCESSED_CSV_PATH, index=False)


def create_jira_issue():
    # Jira API endpoint
    CREATE_ISSUE_URL = f"{JIRA_BASE_URL}/rest/api/3/issue"

    # Authentication
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    if not os.path.exists(PREPROCESSED_CSV_PATH):
        logging.error(f"Preprocessed CSV file not found at {PREPROCESSED_CSV_PATH}")
        print(f"Preprocessed CSV file not found at {PREPROCESSED_CSV_PATH}")
        return
    else:
        logging.info(f"Reading preprocessed CSV file from {PREPROCESSED_CSV_PATH}")
        print(f"Reading preprocessed CSV file from {PREPROCESSED_CSV_PATH}")
        csv_path = PREPROCESSED_CSV_PATH
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            summary = f"New Request for {row['newusername']} - {row['departmentname']} Department"
            description_text = f"""
            Sample Name: {row['samplename']}
            Phone Number: {row['phonenumber']}
            Department: {row['departmentname']}
            Job: {row['job']}
            Email: {row['emailaddress']}
            Cost Center: {row['costcenter']}
            Telephone Lines & Installations: {row['telephonelinesandinstallations']}
            Handsets & Headsets: {row['handsetsandheadsets']}
            Timeframe: {row['timeframe']}
            Date Needed By: {row['dateneededby']}
            Approx. Ending Date: {row['approximateendingdate']}
            Comments: {row['Comments']}
            Created At: {row['createdat']}
            """

            payload = {
                "fields": {
                    "project": {"key": PROJECT_KEY},
                    "summary": summary,
                    "issuetype": {"name": ISSUE_TYPE},
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [
                                    {"type": "text", "text": description_text.strip()}
                                ]
                            }
                        ]
                    },
                }
            }

            response = requests.post(CREATE_ISSUE_URL, auth=auth, headers=headers, json=payload)

            if response.status_code == 201:
                issue_key = response.json()["key"]
                logging.info(f"Created Jira ticket: {issue_key} for {row['newusername']}")
                print(f"Created Jira ticket: {issue_key} for {row['newusername']}")
            else:
                logging.error(f"Failed to create ticket for {row['newusername']}. Error: {response.text}")
                print(f"Failed to create ticket for {row['newusername']}. Error: {response.text}")


def create_jira_issue2(max_retries=3, backoff_factor=5):
    """
    Creates Jira issues from a preprocessed CSV file, with retries on failure.

    Args:
        max_retries (int): Maximum retry attempts for each Jira API request.
        backoff_factor (int): Seconds to wait before retrying, multiplied per attempt.
    """

    # Jira API endpoint
    CREATE_ISSUE_URL = f"{JIRA_BASE_URL}/rest/api/3/issue"

    # Authentication
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    # Check if CSV exists
    if not os.path.exists(PREPROCESSED_CSV_PATH):
        logger.error(f"Preprocessed CSV file not found at {PREPROCESSED_CSV_PATH}")
        return

    logger.info(f"Reading preprocessed CSV file from {PREPROCESSED_CSV_PATH}")
    df = pd.read_csv(PREPROCESSED_CSV_PATH)

    # Loop through each record
    for _, row in df.iterrows():
        summary = f"New Request for {row['newusername']} - {row['departmentname']} Department"
        description_text = f"""
        Sample Name: {row['samplename']}
        Phone Number: {row['phonenumber']}
        Department: {row['departmentname']}
        Job: {row['job']}
        Email: {row['emailaddress']}
        Cost Center: {row['costcenter']}
        Telephone Lines & Installations: {row['telephonelinesandinstallations']}
        Handsets & Headsets: {row['handsetsandheadsets']}
        Timeframe: {row['timeframe']}
        Date Needed By: {row['dateneededby']}
        Approx. Ending Date: {row['approximateendingdate']}
        Comments: {row['Comments']}
        Created At: {row['createdat']}
        """

        payload = {
            "fields": {
                "project": {"key": PROJECT_KEY},
                "summary": summary,
                "issuetype": {"name": ISSUE_TYPE},
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [
                                {"type": "text", "text": description_text.strip()}
                            ],
                        }
                    ],
                },
            }
        }

        # Retries loop
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(CREATE_ISSUE_URL, auth=auth, headers=headers, json=payload)

                if response.status_code == 201:
                    issue_key = response.json()["key"]
                    logger.info(f"Created Jira ticket: {issue_key} for {row['newusername']}")
                    break  # success → stop retrying

                else:
                    logger.warning(
                        f"Attempt {attempt}/{max_retries}: Failed to create ticket for {row['newusername']}. "
                        f"Status: {response.status_code}, Error: {response.text}"
                    )
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt}/{max_retries}: Network error - {e}")

            # Retry with exponential backoff
            if attempt < max_retries:
                sleep_time = backoff_factor * attempt
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"All {max_retries} attempts failed for {row['newusername']}.")



def main():
    rows, columns = get_new_requests()
    if not rows:
        logger.info("No new requests to process.")

    else:
        if os.path.exists(INITIAL_CSV_PATH):
            logger.info("Initial database file found, got new data in last 15 minutes.")
            df_data_fetched = pd.DataFrame(rows, columns=columns)
            df_data_fetched.to_csv(FETCH_FILE_PATH, index=False)
        else:
            logger.info("Initial data file not found. Entire database fetched and will be processed.")
            df_data_fetched = pd.DataFrame(rows, columns=columns)
            df_data_fetched.to_csv(INITIAL_CSV_PATH, index=False)
            df_data_fetched.to_csv(FETCH_FILE_PATH, index=False)


        logger.info("Preprocessing fetched requests...")
        preprocess_request(df_data_fetched)
        logger.info("Preprocessing completed and saved to csv.")

        logger.info("Creating Jira issues...")
        create_jira_issue2()
        logger.info("Jira issues creation completed.")

if __name__ == "__main__":
    main()
  