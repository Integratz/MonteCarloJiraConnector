from jira import JIRA 
import psycopg2
from typing import List, Dict
import time
from dotenv import load_dotenv
import os
from datetime import datetime
from psycopg2.extras import execute_values

class JiraETLConnector:
    def __init__(self, jira_url, username, api_token, db_config, rate_limit_delay=0.5):
        self.jira_url = jira_url
        self.auth = (username, api_token)
        self.db = psycopg2.connect(**db_config)
        self.jira = None
        self.db_conn = None
        self.rate_limit_delay = rate_limit_delay  # 0.5 seconds between API requests to Jira
        
    def create_tables(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS issues_jira (
                issue_key VARCHAR(20) PRIMARY KEY,
                summary TEXT,
                description TEXT,
                status VARCHAR(50),
                assignee VARCHAR(50),
                reporter VARCHAR(50),
                created TIMESTAMP,
                updated TIMESTAMP,
                duedate DATE,
                resolution VARCHAR(50),
                resolutiondate TIMESTAMP,
                priority VARCHAR(50),
                votes INTEGER,
                time_spent INTEGER
                )
            """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transitions_jira (
                issue_key VARCHAR(20),
                from_status VARCHAR(50),
                to_status VARCHAR(50),
                transition_date TIMESTAMP,
                author VARCHAR(100),
                time_in_status_hours FLOAT,
                PRIMARY KEY (issue_key, from_status, to_status, transition_date)
                )
            """)
        
    
    # Connect to Jira using the JIRA library    
    def connect(self):
        try:
            self.jira = JIRA(server=self.jira_url, basic_auth=self.auth)
            return self.jira
        except Exception as e:
            print(f"Failed to connect to Jira: {e}")
            return None
    
    # Fetch projects from Jira    
    def get_projects(self):
        try:
            projects = self.jira.projects()
            return projects
        except Exception as e:
            print(f"Failed to fetch projects: {e}")
            return []
    
    # Fetch issues based on JQL query  
    def get_issues(self, jql_query, max_results=50, max_total_issues=500):
        all_issues = []
        start_at = 0

        while True:
            if len(all_issues) >= max_total_issues:
                print(f"Reached maximum limit of {max_total_issues} issues. Stopping fetch.")
                break

            try:
                issues = self.jira.search_issues(
                    jql_query,
                    startAt=start_at,
                    maxResults=min(max_results, max_total_issues - len(all_issues)),  # Prevent over-fetching
                    expand='changelog'
                )

                if not issues:
                    break

                all_issues.extend(issues)

                if len(issues) < max_results:
                    break  # No more pages
                
                

                start_at += max_results
                time.sleep(self.rate_limit_delay)

            except Exception as e:
                print(f"Failed to fetch issues: {e}")
                break

        return all_issues
   
    # Extract transitions from issues 

    def extract_transitions(self, all_issues) -> List[Dict]:
        transitions = []

        for issue in all_issues:
            changelog = getattr(issue, 'changelog', None)
            if not changelog:
                continue

            # Collect status changes for the current issue
            status_changes = []

            for history in changelog.histories:
                for item in history.items:
                    if item.field == 'status':
                        status_changes.append({
                            'issue_key': issue.key,
                            'from_status': item.fromString,
                            'to_status': item.toString,
                            'transition_date': history.created,
                            'author': history.author.displayName if history.author else 'Unknown'
                        })
            

            # Sort transitions for the current issue
            status_changes.sort(key=lambda x: x['transition_date'])

            # Calculate durations and add to the final transitions list
            for i in range(len(status_changes)):
                current = status_changes[i]
                current_time = datetime.strptime(current['transition_date'], "%Y-%m-%dT%H:%M:%S.%f%z")

                if i + 1 < len(status_changes):
                    next_time = datetime.strptime(status_changes[i + 1]['transition_date'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    time_in_status = (next_time - current_time).total_seconds() / 3600
                else:
                    time_in_status = None

                transitions.append({
                    **current,
                    'transition_date': current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    'time_in_status_hours': round(time_in_status, 2) if time_in_status is not None else None
                })
                
        print(f"Extracted {len(transitions)} transitions from issues.")
        return transitions
        
    # Insert issues into PostgreSQL database    
    def insert_issues_into_db(self, all_issues):
        try:
            cursor = self.db.cursor()
            # Create table issue and transition table if it doesn't exist
            self.create_tables(cursor)
            self.db.commit()
            
            for issue in all_issues:
                data = (
                    issue.key,
                    issue.fields.summary,
                    issue.fields.description,
                    issue.fields.status.name if issue.fields.status else None,
                    issue.fields.assignee.displayName if issue.fields.assignee else None,
                    issue.fields.reporter.displayName if issue.fields.reporter else None,
                    issue.fields.created,
                    issue.fields.updated,
                    issue.fields.duedate,
                    issue.fields.resolution.name if issue.fields.resolution else None,
                    issue.fields.resolutiondate,
                    issue.fields.priority.name if issue.fields.priority else None,
                    issue.fields.timespent
                    )
                
                cursor.execute("""
                    INSERT INTO issues_jira (
                        issue_key, summary, description, status, assignee, reporter,
                        created, updated, duedate, resolution, resolutiondate, priority, 
                        time_spent
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (issue_key) DO UPDATE SET
                        summary = EXCLUDED.summary,
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        assignee = EXCLUDED.assignee,
                        reporter = EXCLUDED.reporter,
                        created = EXCLUDED.created,
                        updated = EXCLUDED.updated,
                        duedate = EXCLUDED.duedate,
                        resolution = EXCLUDED.resolution,
                        resolutiondate = EXCLUDED.resolutiondate,
                        priority = EXCLUDED.priority,
                        time_spent = EXCLUDED.time_spent
                """, data)             
    
            self.db.commit()
            print(f"Inserted {len(all_issues)} issues into the database.")
            
        except Exception as e:
            self.db.rollback()
            print(f"Failed to insert issues into the database: {e}")
            
        finally:
            cursor.close()
            
    def insert_transitions_into_db(self, transitions):
        try:
            cursor = self.db.cursor()
            self.db.commit()
            for transition in transitions:
                cursor.execute("""
                    INSERT INTO transitions_jira (
                        issue_key, from_status, to_status,
                        transition_date, author, time_in_status_hours
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (issue_key, from_status, to_status, transition_date) DO UPDATE SET
                        author = EXCLUDED.author,
                        time_in_status_hours = EXCLUDED.time_in_status_hours
                """, (
                    transition['issue_key'],
                    transition['from_status'],
                    transition['to_status'],
                    transition['transition_date'],
                    transition['author'],
                    transition['time_in_status_hours']
                ))
            self.db.commit()
            print(f"Inserted {len(transitions)} transitions into the database.")
            
        except Exception as e:
            self.db.rollback()
            print(f"Failed to insert transitions into the database: {e}")
        finally:
            cursor.close()
                        

# Main function to run the ETL process        
def main():  
    
    # Function to check if the connection was successful 
    def connection_check(jira_connection):
        if jira_connection:
            print(f"Connected to Jira at {jira_connection.server_info()['baseUrl']}")
            print(f"Jira version: {jira_connection.server_info()['version']}")
        else:
            print("Failed to connect to Jira.")
            
    # Function to print the list of projects
    def print_projects(projects):
        if projects:
            print("\nList of Projects:")
            for project in projects:
                print(f"{project.key}: {project.name}")
        else:
            print("No projects found or failed to fetch projects.")
            
    # Function to prinT issues for a given project
    def print_issues(issues):
        for issue in issues:
            print(f"Issue Key: {issue.key} \n")
            for field_name in issue.raw['fields']:
                value = issue.raw['fields'][field_name]
                if field_name.startswith('customfield_'):
                    continue
                else:
                    print(f"  {field_name} : {value}")
          
    # Function to filter and print specific fields from issues           
    def filter_issues(issues):
        print("\n", "Fetching specific fields from issues...")
        # extracting specific fields from issues
        for issue in issues:
            print(f"Issue Key: {issue.key}")

            # Display the fields you requested
            print(f"  statuscategorychangedate: {issue.fields.statuscategorychangedate}")
            print(f"  statusCategory: {issue.fields.statusCategory}")
            print(f"  lastViewed: {issue.fields.lastViewed}")
            print(f"  priority: {issue.fields.priority.name if issue.fields.priority else 'No priority set'}")
            print(f"  aggregatetimeoriginalestimate: {issue.fields.aggregatetimeoriginalestimate}")
            print(f"  timeestimate: {issue.fields.timeestimate}")
            print(f"  versions: {', '.join([v.name for v in issue.fields.versions]) if issue.fields.versions else 'No versions'}")
            print(f"  assignee: {issue.fields.assignee.displayName if issue.fields.assignee else 'Unassigned'}")
            print(f"  status.name: {issue.fields.status.name}")
            print(f"  creator.displayName: {issue.fields.creator.displayName}")
            print(f"  subtasks: {', '.join([subtask.key for subtask in issue.fields.subtasks]) if issue.fields.subtasks else 'No subtasks'}")
            print(f"  reporter.displayName: {issue.fields.reporter.displayName if issue.fields.reporter else 'No reporter'}")
            #print(f"  progress: {issue.fields.progress.percent if issue.fields.progress else 'No progress'}")
            print(f"  votes: {issue.fields.votes.votes if issue.fields.votes else 'No votes'}")
            print(f"  worklog: {', '.join([log.timeSpent for log in issue.fields.worklog.worklogs]) if issue.fields.worklog.worklogs else 'No worklog'}")
            print(f"  timespent: {issue.fields.timespent}")
            print(f"  resolution: {issue.fields.resolution.name if issue.fields.resolution else 'No resolution'}")
            print(f"  resolutiondate: {issue.fields.resolutiondate}")
            print(f"  created: {issue.fields.created}")
            print(f"  updated: {issue.fields.updated}")
            print(f"  description: {issue.fields.description}")
            print(f"  summary: {issue.fields.summary}")
            print(f"  duedate: {issue.fields.duedate}")
            
            # Extract and display the comments
            if issue.fields.comment.comments:
                for comment in issue.fields.comment.comments:
                    print(f"  comment by {comment.author.displayName}: {comment.body}")
            else:
                print("  No comments")

            print("\n" + "-"*50 + "\n")
    
    
    def print_transitions(transitions):
        if transitions:
            print("\nList of Transitions:")
            for transition in transitions:
                print(f"Issue Key: {transition['issue_key']}, From: {transition['from_status']}, To: {transition['to_status']}, Date: {transition['transition_date']}, Author: {transition['author']}") 
        else:
            print("No transitions found or failed to fetch transitions.")
                
    # Load environment variables from .env file
    load_dotenv()
      
    # Define Jira Server URL and authentication details 
    jira_url = os.getenv("JIRA_URL")
    email = os.getenv("JIRA_EMAIL")
    token = os.getenv("JIRA_API_TOKEN")
    
    if not jira_url or not email or not token:
        print("Please set the JIRA_URL, JIRA_EMAIL, and JIRA_API_TOKEN environment variables.")
        return 
    # Define database configuration
    db_config = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'),  # Default to localhost if not set
        'port': os.getenv('DB_PORT', '5432')  # Default to 5432 if not set
    }



    # Building connector class
    connector = JiraETLConnector(jira_url, email, token, db_config)
    
    # Establish connection
    jira_connection = connector.connect()   
    connection_check(jira_connection)
        
    # Fetch and print projects
    projects = connector.get_projects()
    print_projects(projects)
   
    # Fetch and print all issues from a specific project using JQL
    jql_query = "project=TEST"
    issues = connector.get_issues(jql_query, max_results=10)
    # print_issues(issues)
    
    # Filtering issues
    # filter_issues(issues)
    
    transitions = connector.extract_transitions(issues)
    print_transitions(transitions)
    
    def checking_issue_history(issues):
        print("\nChecking issue history...")
        for issue in issues:
            changelog = getattr(issue, 'changelog', None)
            if not changelog or not changelog.histories:
                continue

            # Just check the first history item to avoid overwhelming output
            first_history = changelog.histories[0]
            
            print("Available attributes in history:")
            for attr in dir(first_history):
                if not attr.startswith('_'):  # Skip internal attributes
                    print(attr)
            
            # Or to get the underlying values (only if it's a custom class or object)
            print("\nHistory attribute values:")
            print(vars(first_history))  # This gives a dict of attribute names and values

        
            for item in first_history.items:
                print("\nAttributes in item:")
                print(vars(item))
                break
            break  # Remove break to check all issues
    
    def checking_fields_in_changelog(all_issues):
        field_set = set()

        for issue in all_issues:
            changelog = getattr(issue, 'changelog', None)
            if not changelog or not changelog.histories:
                continue

            for history in changelog.histories:
                for item in history.items:
                    for status in item.__dict__.keys():
                        field_set.add(status)

        print("Unique fields found in changelog status:")
        for status in sorted(field_set):
            print(status)
    
    # Checking issue history     
    ##checking_issue_history(issues)
            
    checking_fields_in_changelog(issues)
    
    
    # Insert issues into the database
    ##connector.insert_issues_into_db(issues)
    
    # Insert transitions into the database 
    ##connector.insert_transitions_into_db(transitions)
    
    # Close the database connection
    if connector.db:
        connector.db.close()
        print("Database connection closed.")
    else:
        print("No database connection to close.")
    
    
if __name__ == "__main__":
    main()
    
    