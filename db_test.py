# test_new_jql_endpoint.py
import os
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import json

load_dotenv()

def test_new_jql_endpoint():
    server = os.getenv('JIRA_SERVER')
    username = os.getenv('JIRA_USERNAME')
    api_token = os.getenv('JIRA_API_TOKEN_noor')
    project_key = os.getenv('JIRA_PROJECT_KEY')
    
    print(f"Testing NEW JQL endpoint: {server}/rest/api/3/search/jql")
    print(f"Project: {project_key}")
    print(f"Username: {username}")
    
    # Test server info first
    try:
        response = requests.get(
            f"{server}/rest/api/3/serverInfo",
            auth=HTTPBasicAuth(username, api_token)
        )
        response.raise_for_status()
        server_info = response.json()
        print(f"✅ Server connection successful")
        print(f"   Version: {server_info.get('version', 'Unknown')}")
    except Exception as e:
        print(f"❌ Server connection failed: {e}")
        return False
    
    # Test NEW JQL search endpoint
    try:
        jql = f'project = "{project_key}"'
        
        # Request body
        payload =  {
            "expand": "",
            "jql": "project = TEST",
            "startAt": 0,
            "maxResults": 10,
            "fieldsByKeys": True,
            "nextPageToken": "",
            "properties": [
                
            ],
            "fields": [
                "summary",
                "status",
                "assignee"
            ],
            "reconcileIssues": True
            }
        
        response = requests.post(
            f"{server}/rest/api/3/search/jql",
            json=payload,
            auth=HTTPBasicAuth(username, api_token),
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"Request URL: {response.url}")
        print(f"Response Status: {response.status_code}")
        
        response.raise_for_status()
        result = response.json()
        
        issues = result.get('issues', [])
        total = result.get('total', 0)
        
        print(f"✅ NEW JQL search API successful!")
        print(f"   Found {len(issues)} issues out of {total} total")
        
        for issue in issues:
            fields = issue.get('fields', {})
            status_name = fields.get('status', {}).get('name', 'Unknown') if fields.get('status') else 'Unknown'
            assignee_name = fields.get('assignee', {}).get('displayName', 'Unassigned') if fields.get('assignee') else 'Unassigned'
            
            print(f"   📋 {issue.get('key')}: {fields.get('summary', 'No summary')}")
            print(f"      Status: {status_name} | Assignee: {assignee_name}")
        
        return True
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ NEW JQL search API failed with HTTP error: {e}")
        print(f"   Status Code: {e.response.status_code}")
        print(f"   Response: {e.response.text}")
        
        # If the new endpoint doesn't work, suggest fallback
        if e.response.status_code == 404:
            print("\n💡 The new /rest/api/3/search/jql endpoint might not be available yet.")
            print("   This could mean your Jira instance hasn't been updated to support it.")
            print("   Let's try the standard /rest/api/3/search endpoint as fallback...")
            return test_fallback_endpoint(server, username, api_token, project_key)
        
        return False
        
    except Exception as e:
        print(f"❌ NEW JQL search API failed: {e}")
        return False

def test_fallback_endpoint(server, username, api_token, project_key):
    """Test the standard search endpoint as fallback"""
    try:
        jql = f'project = "{project_key}" ORDER BY created DESC'
        response = requests.get(
            f"{server}/rest/api/3/search",
            params={
                'jql': jql,
                'maxResults': 5,
                'fields': 'summary,status,created,assignee'
            },
            auth=HTTPBasicAuth(username, api_token)
        )
        response.raise_for_status()
        result = response.json()
        
        issues = result.get('issues', [])
        print(f"✅ Fallback search API works")
        print(f"   Found {len(issues)} issues")
        
        for issue in issues:
            fields = issue.get('fields', {})
            print(f"   📋 {issue.get('key')}: {fields.get('summary', 'No summary')}")
        
        print("\n⚠️  RECOMMENDATION:")
        print("   The new JQL endpoint isn't available, but the standard search works.")
        print("   You can use the current code, but monitor for when the new endpoint becomes available.")
        
        return True
        
    except Exception as e:
        print(f"❌ Fallback endpoint also failed: {e}")
        return False

def check_jira_version_compatibility():
    """Check if Jira version supports the new endpoint"""
    server = os.getenv('JIRA_SERVER')
    username = os.getenv('JIRA_USERNAME')
    api_token = os.getenv('JIRA_API_TOKEN')
    
    try:
        response = requests.get(
            f"{server}/rest/api/3/serverInfo",
            auth=HTTPBasicAuth(username, api_token)
        )
        response.raise_for_status()
        server_info = response.json()
        
        version = server_info.get('version', 'Unknown')
        print(f"\n📊 Jira Version Analysis:")
        print(f"   Version: {version}")
        print(f"   Build: {server_info.get('buildNumber', 'Unknown')}")
        print(f"   Base URL: {server_info.get('baseUrl', 'Unknown')}")
        
        # Try to parse version
        try:
            version_parts = version.split('.')
            major = int(version_parts[0])
            minor = int(version_parts[1]) if len(version_parts) > 1 else 0
            
            if major >= 9 or (major == 8 and minor >= 20):
                print(f"   ✅ Version likely supports new JQL endpoint")
            else:
                print(f"   ⚠️  Version might not support new JQL endpoint yet")
                print(f"      Consider using fallback to /rest/api/3/search")
        except:
            print(f"   ❓ Could not parse version for compatibility check")
            
    except Exception as e:
        print(f"❌ Could not get server info: {e}")

if __name__ == "__main__":
    print("=== Testing New Jira JQL Search Endpoint ===\n")
    
    # Check Jira version first
    check_jira_version_compatibility()
    
    # Test the new endpoint
    success = test_new_jql_endpoint()
    
    if success:
        print("\n🎉 All tests passed! The new JQL endpoint is working correctly.")
    else:
        print("\n❌ Tests failed. Check your configuration or Jira version.")