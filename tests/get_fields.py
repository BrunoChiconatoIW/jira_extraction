import os
from jira import JIRA # type: ignore
from dotenv import load_dotenv # type: ignore

load_dotenv()

jira_server = os.getenv('JIRA_SERVER')
email = os.getenv('JIRA_EMAIL')
api_token = os.getenv('JIRA_API_TOKEN')
board_id = os.getenv('JIRA_BOARD_ID')

jira = JIRA(server=jira_server, basic_auth=(email, api_token))

issue_key = 'IW-696'

issue = jira.issue(issue_key)

for field_name, field_value in issue.fields.__dict__.items():
    print(f"{field_name}: {field_value}")