# Data Engineering Community LaunchPad Python task 1

## Project Title
Task is to build an automated quote email delivery platform using ZenQuotes API. Get quotes from the API and automate daily sending to subscribers’ email

## Project Overview
The project is for MindFuel Company, a mental wellness startup.
They want to send motivational quotes to subscribed users every morning at 7 AM.

Task is to build a production-grade backend service that:
- Pulls a new quote daily from ZenQuotes API
- Personalizes and sends it to subscribed users via email
- Logs activity and handles failures
- Can scale to hundreds or thousands of users

## Deliverables
### 1. ZenQuotes API
- Fetch quote from the API: https://zenquotes.io/
  - Handle edge cases:
  - No response
  - Malformed response
### 2. User Subscription Management
- Create a users table in any database management system of your choice (e.g., Postgres, MySQL, SQL Server).
- Each user should have:
  - Email address
  - Name
  - Subscription status (active/inactive)
  - Email frequency preference (daily/weekly)
### 3. Email Module
- Send personalized quotes
- Handle failures and retries
- Log email delivery status per user
### 4. Logging & Monitoring
- Log events to files
- Quote fetched
- Email sent (to which user, when)
- Failures and retry attempts
- Send email summary logs to admin (e.g., daily email with stats)

## Requirements for project:
Clone Project directory
```
git clone https://github.com/kabiromohd/data_engineering_python_task.git

cd kabiromohd/data_engineering_python_task/Task-1

pip install -r requirements.txt
```

# Create the users database
Open ```database_setup.py``` and populate valid users details and save
run below command to setup a duckdb database:
```
python database_setup.py
```
# Setup of email service
- Populate the credentials in the .env file
- setup a cron job as in below screenshot

<img width="1366" height="768" alt="image" src="https://github.com/user-attachments/assets/19a8484d-4c5c-4be2-9f80-70e4444ff8f5" />

