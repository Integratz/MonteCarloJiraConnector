import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError
from jira import JIRA
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
        
        # Initialize Jira client
        self.jira = JIRA(
            server=jira_config.server,
            basic_auth=(jira_config.username, jira_config.api_token)
        )
        
        # Initialize DynamoDB client
        # Option 1: Use default credentials (recommended)
        self.dynamodb = boto3.resource('dynamodb', region_name=dynamodb_config.region)
        
        # Option 2: Use explicit credentials (not recommended for production)
        # self.dynamodb = boto3.resource(
        #     'dynamodb',
        #     region_name=dynamodb_config.region,
        #     aws_access_key_id='your_access_key_here',
        #     aws_secret_access_key='your_secret_access_key_here'
        # )
        
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

    def extract_jira_issues(self, days_back: int = 30) -> List[Dict[str, Any]]:
        """Extract raw issue data from Jira using modern search API"""
        logger.info(f"Extracting Jira issues for the last {days_back} days")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # JQL query to get issues - using modern API
        jql = f'project = "{self.jira_config.project_key}" AND updated >= "{start_date.strftime("%Y-%m-%d")}" ORDER BY created DESC'
        
        issues = []
        start_at = 0
        max_results = 50  # Reduced for better performance
        
        while True:
            try:
                # Use the modern search_issues API with proper parameters
                batch = self.jira.search_issues(
                    jql_str=jql,
                    startAt=start_at,
                    maxResults=max_results,
                    expand='changelog,names',
                    fields='summary,description,status,assignee,reporter,priority,issuetype,created,updated,resolution,labels,components,customfield_10016'
                )
                
                if not batch:
                    break
                
                for issue in batch:
                    # Safely extract issue data with better error handling
                    issue_data = {
                        'issue_id': issue.key,
                        'summary': getattr(issue.fields, 'summary', '') or '',
                        'description': self._safe_get_description(issue.fields),
                        'status': getattr(issue.fields.status, 'name', 'Unknown') if hasattr(issue.fields, 'status') else 'Unknown',
                        'assignee': self._safe_get_user_name(getattr(issue.fields, 'assignee', None)),
                        'reporter': self._safe_get_user_name(getattr(issue.fields, 'reporter', None)),
                        'priority': getattr(issue.fields.priority, 'name', None) if hasattr(issue.fields, 'priority') and issue.fields.priority else None,
                        'issue_type': getattr(issue.fields.issuetype, 'name', 'Unknown') if hasattr(issue.fields, 'issuetype') else 'Unknown',
                        'created': getattr(issue.fields, 'created', None),
                        'updated': getattr(issue.fields, 'updated', None),
                        'resolution': getattr(issue.fields.resolution, 'name', None) if hasattr(issue.fields, 'resolution') and issue.fields.resolution else None,
                        'story_points': self._safe_get_story_points(issue.fields),
                        'labels': getattr(issue.fields, 'labels', []) or [],
                        'components': self._safe_get_components(getattr(issue.fields, 'components', [])),
                        'extract_timestamp': datetime.now().isoformat()
                    }
                    issues.append(issue_data)
                
                start_at += max_results
                if len(batch) < max_results:
                    break
                    
            except Exception as e:
                logger.error(f"Error extracting issues: {e}")
                break
        
        logger.info(f"Extracted {len(issues)} issues")
        return issues
    
    def _safe_get_description(self, fields):
        """Safely extract description from issue fields"""
        try:
            desc = getattr(fields, 'description', None)
            if desc is None:
                return ''
            # Handle different description formats
            if hasattr(desc, 'content'):
                # New Atlassian Document Format
                return str(desc)
            else:
                # Plain text or old format
                return str(desc) if desc else ''
        except Exception:
            return ''
    
    def _safe_get_user_name(self, user):
        """Safely extract user display name"""
        if user is None:
            return None
        try:
            return getattr(user, 'displayName', None) or getattr(user, 'name', None)
        except Exception:
            return None
    
    def _safe_get_story_points(self, fields):
        """Safely extract story points from various custom fields"""
        # Common story points field IDs - adjust as needed for your Jira instance
        story_point_fields = ['customfield_10016', 'customfield_10002', 'customfield_10004']
        
        for field_id in story_point_fields:
            try:
                value = getattr(fields, field_id, None)
                if value is not None:
                    return float(value) if isinstance(value, (int, float)) else None
            except Exception:
                continue
        return None
    
    def _safe_get_components(self, components):
        """Safely extract component names"""
        try:
            if not components:
                return []
            return [getattr(c, 'name', str(c)) for c in components]
        except Exception:
            return []

    def extract_jira_transitions(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract status change transitions from Jira issues using modern API"""
        logger.info("Extracting Jira transitions")
        
        transitions = []
        
        for issue_data in issues:
            try:
                # Use the modern API to get issue with changelog
                issue = self.jira.issue(
                    issue_data['issue_id'], 
                    expand='changelog',
                    fields='key'
                )
                
                # Check if changelog exists
                if not hasattr(issue, 'changelog') or not issue.changelog:
                    continue
                
                # Process changelog histories
                for history in issue.changelog.histories:
                    if not hasattr(history, 'items') or not history.items:
                        continue
                        
                    for item in history.items:
                        # Only process status changes
                        if getattr(item, 'field', None) == 'status':
                            transition_data = {
                                'issue_id': issue.key,
                                'transition_timestamp': getattr(history, 'created', datetime.now().isoformat()),
                                'from_status': getattr(item, 'fromString', None) or 'Unknown',
                                'to_status': getattr(item, 'toString', None) or 'Unknown',
                                'author': self._safe_get_user_name(getattr(history, 'author', None)) or 'Unknown',
                                'transition_date': (getattr(history, 'created', datetime.now().isoformat()))[:10],  # YYYY-MM-DD format
                                'extract_timestamp': datetime.now().isoformat()
                            }
                            transitions.append(transition_data)
                            
            except Exception as e:
                logger.error(f"Error extracting transitions for {issue_data['issue_id']}: {e}")
                continue
        
        logger.info(f"Extracted {len(transitions)} transitions")
        return transitions

    def calculate_flow_metrics(self, issues: List[Dict[str, Any]], transitions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate team flow metrics"""
        logger.info("Calculating flow metrics")
        
        metrics = []
        
        # Group issues by date and calculate daily metrics
        daily_data = {}
        
        for issue in issues:
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
                # Find when issue was started (moved to In Progress)
                start_transition = next(
                    (t for t in transitions 
                     if t['issue_id'] == issue_id and t['to_status'] in ['In Progress', 'In Development']),
                    None
                )
                if start_transition:
                    start_date = datetime.fromisoformat(start_transition['transition_timestamp'].replace('Z', '+00:00'))
                    end_date = datetime.fromisoformat(transition['transition_timestamp'].replace('Z', '+00:00'))
                    cycle_time = (end_date - start_date).days
                    cycle_times[issue_id] = cycle_time
        
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
                'confidence_level': Decimal('0.70'),  # 70% confidence
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

    def run_extraction(self, days_back: int = 10000):
        """Run the complete extraction process"""
        logger.info("Starting Jira to DynamoDB extraction")
        
        try:
            # Create tables if needed
            self.create_tables_if_not_exist()
            
            # Extract raw issues
            issues = self.extract_jira_issues(days_back)
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
            server=os.getenv('JIRA_SERVER', 'https://integratz.atlassian.net'),
            username=os.getenv('JIRA_USERNAME', 'noor@integratz.com'),
            api_token=os.getenv('JIRA_API_TOKEN2', 'API_KEY'),
            project_key=os.getenv('JIRA_PROJECT_KEY', 'TEST')
        )

    dynamodb_config = DynamoDBConfig(
        region=os.getenv('AWS_REGION', 'us-east-1'),
        table_prefix=os.getenv('DYNAMODB_TABLE_PREFIX', 'jira')
        )

    extractor = JiraToDynamoDB(jira_config, dynamodb_config)

    extractor.run_extraction(days_back=30)

if __name__ == "__main__":
        main()
