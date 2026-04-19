"""
MCP Client for Strands ETL
Connects Lambda functions to MCP servers for enhanced agent capabilities
"""

import json
import subprocess
import os
from typing import Dict, List, Any, Optional


class MCPClient:
    """
    Client for communicating with MCP servers from Lambda functions

    Usage:
        client = MCPClient()
        result = client.call_tool('aws', 'query_execution_history', {
            'job_name': 'customer_order_summary',
            'limit': 10
        })
    """

    def __init__(self, server_configs_dir: str = '/tmp/mcp/servers'):
        """
        Initialize MCP client

        Args:
            server_configs_dir: Directory containing server configuration files
        """
        self.server_configs_dir = server_configs_dir
        self.servers = {}
        self._load_server_configs()

    def _load_server_configs(self):
        """Load all MCP server configurations"""
        if not os.path.exists(self.server_configs_dir):
            return

        for filename in os.listdir(self.server_configs_dir):
            if filename.endswith('-server.json'):
                config_path = os.path.join(self.server_configs_dir, filename)
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    self.servers.update(config.get('mcpServers', {}))

    def list_available_servers(self) -> List[str]:
        """List all available MCP servers"""
        return list(self.servers.keys())

    def get_server_capabilities(self, server_name: str) -> List[str]:
        """Get capabilities of a specific server"""
        if server_name not in self.servers:
            raise ValueError(f"Server {server_name} not found")
        return self.servers[server_name].get('capabilities', [])

    def call_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Call a tool on an MCP server

        Args:
            server_name: Name of the MCP server
            tool_name: Name of the tool to call
            arguments: Tool arguments

        Returns:
            Tool response
        """
        if server_name not in self.servers:
            raise ValueError(f"Server {server_name} not found")

        server_config = self.servers[server_name]

        # Prepare MCP request
        request = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'tools/call',
            'params': {
                'name': tool_name,
                'arguments': arguments
            }
        }

        # Execute MCP server command
        try:
            command = [server_config['command']] + server_config.get('args', [])
            env = os.environ.copy()
            env.update(server_config.get('env', {}))

            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )

            stdout, stderr = process.communicate(
                input=json.dumps(request).encode('utf-8'),
                timeout=30
            )

            if process.returncode != 0:
                raise Exception(f"MCP server error: {stderr.decode('utf-8')}")

            response = json.loads(stdout.decode('utf-8'))

            if 'error' in response:
                raise Exception(f"Tool error: {response['error']}")

            return response.get('result', {})

        except subprocess.TimeoutExpired:
            process.kill()
            raise Exception(f"MCP server timeout for {server_name}")
        except Exception as e:
            raise Exception(f"Failed to call {tool_name} on {server_name}: {str(e)}")

    def query_execution_history(
        self,
        job_name: str,
        limit: int = 10,
        status: str = 'all'
    ) -> Dict[str, Any]:
        """Query job execution history via AWS MCP server"""
        return self.call_tool('aws', 'query_execution_history', {
            'job_name': job_name,
            'limit': limit,
            'status': status
        })

    def get_cost_trends(
        self,
        job_name: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get cost trends via AWS MCP server"""
        return self.call_tool('aws', 'get_cost_trends', {
            'job_name': job_name,
            'days': days
        })

    def get_quality_trends(
        self,
        job_name: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get quality trends via AWS MCP server"""
        return self.call_tool('aws', 'get_quality_trends', {
            'job_name': job_name,
            'days': days
        })

    def list_learning_vectors(
        self,
        limit: int = 100,
        prefix: str = 'learning/vectors/learning/'
    ) -> Dict[str, Any]:
        """List learning vectors via AWS MCP server"""
        return self.call_tool('aws', 'list_learning_vectors', {
            'limit': limit,
            'prefix': prefix
        })

    def query_issues(
        self,
        issue_type: Optional[str] = None,
        job_name: Optional[str] = None,
        severity: Optional[str] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """Query issues via AWS MCP server"""
        args = {'limit': limit}
        if issue_type:
            args['issue_type'] = issue_type
        if job_name:
            args['job_name'] = job_name
        if severity:
            args['severity'] = severity

        return self.call_tool('aws', 'query_issues', args)

    def read_file(self, file_path: str) -> str:
        """Read file via filesystem MCP server"""
        result = self.call_tool('filesystem', 'read_file', {
            'path': file_path
        })
        return result.get('content', [{}])[0].get('text', '')

    def search_code(
        self,
        query: str,
        language: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search code via GitHub MCP server"""
        args = {'query': query}
        if language:
            args['language'] = language

        result = self.call_tool('github', 'search_code', args)
        return result.get('results', [])

    def send_slack_message(
        self,
        message: str,
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send Slack message via Slack MCP server"""
        args = {'message': message}
        if channel:
            args['channel'] = channel

        return self.call_tool('slack', 'send_message', args)


# Singleton instance
_mcp_client = None

def get_mcp_client() -> MCPClient:
    """Get singleton MCP client instance"""
    global _mcp_client
    if _mcp_client is None:
        _mcp_client = MCPClient()
    return _mcp_client


# Example usage in Lambda
def example_lambda_handler(event, context):
    """
    Example Lambda function using MCP client
    """
    client = get_mcp_client()

    # Query execution history
    history = client.query_execution_history('customer_order_summary', limit=5)
    print(f"Recent executions: {history}")

    # Get cost trends
    costs = client.get_cost_trends('customer_order_summary', days=30)
    print(f"Cost trends: {costs}")

    # Query issues
    issues = client.query_issues(job_name='customer_order_summary', severity='high')
    print(f"High severity issues: {issues}")

    # Read a file
    script_content = client.read_file('/home/user/strands_etl/pyscript/customer_order_summary_glue.py')
    print(f"Script length: {len(script_content)}")

    # Send Slack notification
    client.send_slack_message(
        f"Job completed with {len(issues['issues'])} issues detected",
        channel="#strands-etl-alerts"
    )

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'MCP operations completed'})
    }
