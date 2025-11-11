# Data Engineering Community LaunchPad Python task 2

## COW JACKET: PYTHON AUTOMATION

At Cowjacket, users raise support requests from our web form, and each request needs to be assigned promptly to internal staff.
their workflows are managed through Jira, but here’s our big headache:
the Jira instance and our web form database don't talk to each other directly.

Their support staffer have to receive a daily CSV dump and manually create Jira tickets for every single request, 
a solution that’s now officially creaking under pressure. Not only is this painfully slow (with a 24-hour delay!),
but it’s also impossible to scale.

## What the Company wants done.
They need to automate this process and rescue the support team from hours of tedious manual work. 
Automating this will drastically reduce turnaround time for request resolutions and keep them in line with team SLA.

Here’s the breakdown from the Head of Support:
- Every form submission from their website needs to automatically become a Jira ticket in their Customer Service Desk.
- Their Jira project has a form embedded, and will have to be map their website form fields to Jira fields.
- Tickets must be created within 15 minutes.

## Automation of process
A python script (```task3-notebook.py```) has been develop to handle the able process. A cron job has also been setup to run every 15minutes as shown below

<img width="1366" height="768" alt="image" src="https://github.com/user-attachments/assets/677b0ecd-9b6a-4be4-9464-92de9caebcc6" />


