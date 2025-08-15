import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError
import requests
from requests.auth import HTTPBasicAuth
import os
from dataclasses import dataclass, asdict
from decimal import Decimal
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class JiraConfig:
    server: str
    username: str
    api_token: str
    project_key: str

@dataclass
class DynamoDBConfig:
    region: str
    table_prefix: str = "jira"

class JiraToDynamoDB:
    def __init__(self, jira_config: JiraConfig, dynamodb_config: DynamoDBConfig):
        self.jira_config = jira_config
        self.dynamodb_config = dynamodb_config
        
        # Initialize DynamoDB client
        self.dynamodb = boto3.resource('dynamodb', region_name=dynamodb_config.region)
        
        # Table names
        self.tables = {
            'issues': f"{dynamodb_config.table_prefix}_issues",
            'transitions': f"{dynamodb_config.table_prefix}_transitions",
            'metrics': f"{dynamodb_config.table_prefix}_flow_metrics",
            'forecast': f"{dynamodb_config.table_prefix}_forecast_items"
        }

    def create_tables_if_not_exist(self):
        """Create DynamoDB tables if they don't exist"""
        table_schemas = {
            'issues': {
                'TableName': self.tables['issues'],
                'KeySchema': [
                    {'AttributeName': 'issue_id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'issue_id', 'AttributeType': 'S'}
                ]
            },
            'transitions': {
                'TableName': self.tables['transitions'],
                'KeySchema': [
                    {'AttributeName': 'issue_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'transition_timestamp', 'KeyType': 'RANGE'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'issue_id', 'AttributeType': 'S'},
                    {'AttributeName': 'transition_timestamp', 'AttributeType': 'S'}
                ]
            },
            'metrics': {
                'TableName': self.tables['metrics'],
                'KeySchema': [
                    {'AttributeName': 'team_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'date', 'KeyType': 'RANGE'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'team_id', 'AttributeType': 'S'},
                    {'AttributeName': 'date', 'AttributeType': 'S'}
                ]
            },
            'forecast': {
                'TableName': self.tables['forecast'],
                'KeySchema': [
                    {'AttributeName': 'forecast_id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'forecast_id', 'AttributeType': 'S'}
                ]
            }
        }
        
        for table_type, schema in table_schemas.items():
            try:
                table = self.dynamodb.create_table(
                    **schema,
                    BillingMode='PAY_PER_REQUEST'
                )
                logger.info(f"Created table: {schema['TableName']}")
                table.wait_until_exists()
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceInUseException':
                    logger.info(f"Table {schema['TableName']} already exists")
                else:
                    logger.error(f"Error creating table {schema['TableName']}: {e}")
                    raise

    def search_issues_new_endpoint(self, jql: str, start_at: int = 0, max_results: int = 50) -> Dict[str, Any]:
        """Use the new JQL search endpoint with minimal payload"""
        try:
            # Minimal payload that works
            payload = {
                "jql": jql,
                "maxResults": max_results,
                "fields": [
                    "summary",
                    "description", 
                    "status",
                    "assignee",
                    "reporter",
                    "priority",
                    "issuetype",
                    "created",
                    "updated",
                    "resolution",
                    "labels",
                    "components"  # Story points field - adjust as needed
                ]
            }
            
            response = requests.post(
                f"{self.jira_config.server}/rest/api/3/search/jql",
                json=payload,
                auth=HTTPBasicAuth(self.jira_config.username, self.jira_config.api_token),
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code != 200:
                logger.error(f"New JQL endpoint failed: {response.status_code} - {response.text}")
                # Fallback to standard endpoint
                return self.search_issues_fallback(jql, start_at, max_results)
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error with new JQL endpoint: {e}")
            # Fallback to standard endpoint
            return self.search_issues_fallback(jql, start_at, max_results)

    def search_issues_fallback(self, jql: str, start_at: int = 0, max_results: int = 50) -> Dict[str, Any]:
        """Fallback to standard search endpoint"""
        try:
            response = requests.get(
                f"{self.jira_config.server}/rest/api/3/search",
                params={
                    'jql': jql,
                    'startAt': start_at,
                    'maxResults': max_results,
                    'fields': 'summary,description,status,assignee,reporter,priority,issuetype,created,updated,resolution,labels,components,customfield_10016'
                },
                auth=HTTPBasicAuth(self.jira_config.username, self.jira_config.api_token)
            )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Fallback endpoint also failed: {e}")
            raise

    def extract_jira_issues(self, days_back: int = 30) -> List[Dict[str, Any]]:
        """Extract raw issue data from Jira using new search API"""
        logger.info(f"Extracting Jira issues for the last {days_back} days")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # JQL query to get issues
        jql = f'project = "{self.jira_config.project_key}" AND updated >= "{start_date.strftime("%Y-%m-%d")}" ORDER BY created DESC'
        
        issues = []
        start_at = 0
        max_results = 50
        
        while True:
            try:
                # Use new JQL endpoint
                result = self.search_issues_new_endpoint(jql, start_at, max_results)
                
                batch = result.get('issues', [])
                if not batch:
                    break
                
                for issue_raw in batch:
                    fields = issue_raw.get('fields', {})
                    
                    # Extract issue data safely
                    issue_data = {
                        'issue_id': issue_raw.get('key', ''),
                        'summary': fields.get('summary', '') or '',
                        'description': self._safe_get_description(fields.get('description')),
                        'status': self._safe_get_nested_field(fields, 'status', 'name', 'Unknown'),
                        'assignee': self._safe_get_user_name(fields.get('assignee')),
                        'reporter': self._safe_get_user_name(fields.get('reporter')),
                        'priority': self._safe_get_nested_field(fields, 'priority', 'name'),
                        'issue_type': self._safe_get_nested_field(fields, 'issuetype', 'name', 'Unknown'),
                        'created': fields.get('created'),
                        'updated': fields.get('updated'),
                        'resolution': self._safe_get_nested_field(fields, 'resolution', 'name'),
                        'story_points': self._safe_get_story_points(fields),
                        'labels': fields.get('labels', []) or [],
                        'components': self._safe_get_components(fields.get('components', [])),
                        'extract_timestamp': datetime.now().isoformat()
                    }
                    issues.append(issue_data)
                
                start_at += max_results
                total = result.get('total', 0)
                
                logger.info(f"Extracted {len(issues)}/{total} issues so far...")
                
                if len(batch) < max_results or start_at >= total:
                    break
                    
            except Exception as e:
                logger.error(f"Error extracting issues: {e}")
                break
        
        logger.info(f"Extracted {len(issues)} total issues")
        return issues

    def get_issue_changelog(self, issue_key: str) -> Dict[str, Any]:
        """Get issue changelog using REST API"""
        try:
            response = requests.get(
                f"{self.jira_config.server}/rest/api/3/issue/{issue_key}",
                params={
                    'expand': 'changelog',
                    'fields': 'key'
                },
                auth=HTTPBasicAuth(self.jira_config.username, self.jira_config.api_token)
            )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting changelog for {issue_key}: {e}")
            return {}

    def extract_jira_transitions(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract status change transitions from Jira issues using REST API"""
        logger.info("Extracting Jira transitions")
        
        transitions = []
        
        for i, issue_data in enumerate(issues):
            if i % 10 == 0:
                logger.info(f"Processing transitions for issue {i+1}/{len(issues)}")
                
            try:
                issue_with_changelog = self.get_issue_changelog(issue_data['issue_id'])
                changelog = issue_with_changelog.get('changelog', {})
                histories = changelog.get('histories', [])
                
                for history in histories:
                    items = history.get('items', [])
                    
                    for item in items:
                        # Only process status changes
                        if item.get('field') == 'status':
                            author = history.get('author', {})
                            
                            transition_data = {
                                'issue_id': issue_data['issue_id'],
                                'transition_timestamp': history.get('created', datetime.now().isoformat()),
                                'from_status': item.get('fromString', 'Unknown'),
                                'to_status': item.get('toString', 'Unknown'),
                                'author': author.get('displayName', 'Unknown'),
                                'transition_date': (history.get('created', datetime.now().isoformat()))[:10],
                                'extract_timestamp': datetime.now().isoformat()
                            }
                            transitions.append(transition_data)
                            
            except Exception as e:
                logger.error(f"Error extracting transitions for {issue_data['issue_id']}: {e}")
                continue
        
        logger.info(f"Extracted {len(transitions)} transitions")
        return transitions
    
    def _safe_get_description(self, description):
        """Safely extract description"""
        if description is None:
            return ''
        if isinstance(description, dict):
            return str(description)
        return str(description) if description else ''
    
    def _safe_get_user_name(self, user):
        """Safely extract user display name"""
        if user is None:
            return None
        return user.get('displayName') or user.get('name')
    
    def _safe_get_nested_field(self, fields, field_name, sub_field, default=None):
        """Safely get nested field value"""
        field = fields.get(field_name)
        if field is None:
            return default
        return field.get(sub_field, default)
    
    def _safe_get_story_points(self, fields):
        """Safely extract story points from various custom fields"""
        story_point_fields = ['customfield_10016', 'customfield_10002', 'customfield_10004']
        
        for field_id in story_point_fields:
            value = fields.get(field_id)
            if value is not None:
                try:
                    return float(value) if isinstance(value, (int, float)) else None
                except (ValueError, TypeError):
                    continue
        return None
    
    def _safe_get_components(self, components):
        """Safely extract component names"""
        try:
            if not components:
                return []
            return [c.get('name', str(c)) for c in components if isinstance(c, dict)]
        except Exception:
            return []

    def calculate_flow_metrics(self, issues: List[Dict[str, Any]], transitions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate team flow metrics"""
        logger.info("Calculating flow metrics")
        
        metrics = []
        
        # Group issues by date and calculate daily metrics
        daily_data = {}
        
        for issue in issues:
            if not issue.get('created'):
                continue
                
            created_date = issue['created'][:10]
            if created_date not in daily_data:
                daily_data[created_date] = {
                    'issues_created': 0,
                    'issues_completed': 0,
                    'total_story_points': 0,
                    'completed_story_points': 0
                }
            
            daily_data[created_date]['issues_created'] += 1
            if issue['story_points']:
                daily_data[created_date]['total_story_points'] += issue['story_points']
            
            # Check if issue is completed
            if issue['status'] in ['Done', 'Closed', 'Resolved']:
                daily_data[created_date]['issues_completed'] += 1
                if issue['story_points']:
                    daily_data[created_date]['completed_story_points'] += issue['story_points']
        
        # Calculate cycle times from transitions
        cycle_times = {}
        for transition in transitions:
            if transition['to_status'] in ['Done', 'Closed', 'Resolved']:
                issue_id = transition['issue_id']
                # Find when issue was started
                start_transition = next(
                    (t for t in transitions 
                     if t['issue_id'] == issue_id and t['to_status'] in ['In Progress', 'In Development']),
                    None
                )
                if start_transition:
                    try:
                        start_date = datetime.fromisoformat(start_transition['transition_timestamp'].replace('Z', '+00:00'))
                        end_date = datetime.fromisoformat(transition['transition_timestamp'].replace('Z', '+00:00'))
                        cycle_time = (end_date - start_date).days
                        cycle_times[issue_id] = cycle_time
                    except ValueError:
                        continue
        
        # Create metrics records
        for date, data in daily_data.items():
            avg_cycle_time = sum(cycle_times.values()) / len(cycle_times) if cycle_times else 0
            
            metric = {
                'team_id': self.jira_config.project_key,
                'date': date,
                'issues_created': Decimal(str(data['issues_created'])),
                'issues_completed': Decimal(str(data['issues_completed'])),
                'total_story_points': Decimal(str(data['total_story_points'])),
                'completed_story_points': Decimal(str(data['completed_story_points'])),
                'avg_cycle_time': Decimal(str(round(avg_cycle_time, 2))),
                'throughput': Decimal(str(data['issues_completed'])),
                'extract_timestamp': datetime.now().isoformat()
            }
            metrics.append(metric)
        
        logger.info(f"Calculated {len(metrics)} flow metrics")
        return metrics

    def generate_forecast_items(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate simple forecast based on historical metrics"""
        logger.info("Generating forecast items")
        
        if not metrics:
            return []
        
        # Calculate average throughput
        total_throughput = sum(float(m['throughput']) for m in metrics)
        avg_throughput = total_throughput / len(metrics) if metrics else 0
        
        # Calculate average cycle time
        total_cycle_time = sum(float(m['avg_cycle_time']) for m in metrics)
        avg_cycle_time = total_cycle_time / len(metrics) if metrics else 0
        
        # Generate forecast for next 30 days
        forecasts = []
        base_date = datetime.now()
        
        for i in range(1, 31):
            forecast_date = base_date + timedelta(days=i)
            
            forecast = {
                'forecast_id': f"{self.jira_config.project_key}_{forecast_date.strftime('%Y%m%d')}",
                'team_id': self.jira_config.project_key,
                'forecast_date': forecast_date.strftime('%Y-%m-%d'),
                'predicted_throughput': Decimal(str(round(avg_throughput, 2))),
                'predicted_cycle_time': Decimal(str(round(avg_cycle_time, 2))),
                'confidence_level': Decimal('0.70'),
                'forecast_type': 'throughput_based',
                'created_at': datetime.now().isoformat()
            }
            forecasts.append(forecast)
        
        logger.info(f"Generated {len(forecasts)} forecast items")
        return forecasts

    def save_to_dynamodb(self, table_name: str, items: List[Dict[str, Any]]):
        """Save items to DynamoDB table"""
        if not items:
            return
        
        table = self.dynamodb.Table(table_name)
        
        # Batch write items
        with table.batch_writer() as batch:
            for item in items:
                try:
                    # Convert any float values to Decimal for DynamoDB
                    item_converted = self.convert_floats_to_decimal(item)
                    batch.put_item(Item=item_converted) 
                except Exception as e:
                    logger.error(f"Error saving item to {table_name}: {e}")
        
        logger.info(f"Saved {len(items)} items to {table_name}")

    def convert_floats_to_decimal(self, obj):
        """Convert float values to Decimal for DynamoDB compatibility"""
        if isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        elif isinstance(obj, float):
            return Decimal(str(obj))
        else:
            return obj

    def run_extraction(self, days_back: int = 200):
        """Run the complete extraction process"""
        logger.info("Starting Jira to DynamoDB extraction")
        
        try:
            # Create tables if needed
            self.create_tables_if_not_exist()
            
            # Extract raw issues
            issues = self.extract_jira_issues(days_back)
            if not issues:
                logger.warning("No issues found. Check your project key and permissions.")
                return
                
            self.save_to_dynamodb(self.tables['issues'], issues)
            
            # Extract transitions
            transitions = self.extract_jira_transitions(issues)
            self.save_to_dynamodb(self.tables['transitions'], transitions)
            
            # Calculate flow metrics
            metrics = self.calculate_flow_metrics(issues, transitions)
            self.save_to_dynamodb(self.tables['metrics'], metrics)
            
            # Generate forecasts
            forecasts = self.generate_forecast_items(metrics)
            self.save_to_dynamodb(self.tables['forecast'], forecasts)
            
            logger.info("Extraction completed successfully")
            
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            raise

def main():
    jira_config = JiraConfig(
        server = os.getenv('JIRA_SERVER'),
        username = os.getenv('JIRA_USERNAME'),
        api_token = os.getenv('JIRA_API_TOKEN'),
        project_key = os.getenv('JIRA_PROJECT_KEY')
    )

    dynamodb_config = DynamoDBConfig(
        region=os.getenv('AWS_REGION', 'us-east-1'),
        table_prefix=os.getenv('DYNAMODB_TABLE_PREFIX', 'jira')
    )

    extractor = JiraToDynamoDB(jira_config, dynamodb_config)
    extractor.run_extraction(days_back=356)

if __name__ == "__main__":
    main()