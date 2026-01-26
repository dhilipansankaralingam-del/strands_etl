#!/usr/bin/env node
/**
 * AWS MCP Server for Strands ETL
 * Provides AWS service tools for Bedrock Agents via MCP protocol
 */

const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} = require('@modelcontextprotocol/sdk/types.js');

const AWS = require('aws-sdk');

// Configure AWS SDK
const region = process.env.AWS_REGION || 'us-east-1';
AWS.config.update({ region });

const dynamodb = new AWS.DynamoDB.DocumentClient();
const glue = new AWS.Glue();
const s3 = new AWS.S3();
const cloudwatch = new AWS.CloudWatch();
const ses = new AWS.SES();

// MCP Server
const server = new Server(
  {
    name: 'strands-aws-server',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Tool definitions
const tools = [
  {
    name: 'query_execution_history',
    description: 'Query job execution history from DynamoDB',
    inputSchema: {
      type: 'object',
      properties: {
        job_name: {
          type: 'string',
          description: 'Name of the job to query',
        },
        limit: {
          type: 'number',
          description: 'Number of recent executions to return',
          default: 10,
        },
        status: {
          type: 'string',
          description: 'Filter by status (success, failed, running)',
          enum: ['success', 'failed', 'running', 'all'],
          default: 'all',
        },
      },
      required: ['job_name'],
    },
  },
  {
    name: 'get_cost_trends',
    description: 'Get cost trends for a job over time',
    inputSchema: {
      type: 'object',
      properties: {
        job_name: {
          type: 'string',
          description: 'Name of the job',
        },
        days: {
          type: 'number',
          description: 'Number of days to look back',
          default: 30,
        },
      },
      required: ['job_name'],
    },
  },
  {
    name: 'get_quality_trends',
    description: 'Get data quality trends for a job',
    inputSchema: {
      type: 'object',
      properties: {
        job_name: {
          type: 'string',
          description: 'Name of the job',
        },
        days: {
          type: 'number',
          description: 'Number of days to look back',
          default: 30,
        },
      },
      required: ['job_name'],
    },
  },
  {
    name: 'get_glue_job_status',
    description: 'Check current status of a Glue job',
    inputSchema: {
      type: 'object',
      properties: {
        job_name: {
          type: 'string',
          description: 'Glue job name',
        },
        run_id: {
          type: 'string',
          description: 'Optional specific run ID',
        },
      },
      required: ['job_name'],
    },
  },
  {
    name: 'list_learning_vectors',
    description: 'List available learning vectors from S3',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of vectors to return',
          default: 100,
        },
        prefix: {
          type: 'string',
          description: 'S3 key prefix to filter',
          default: 'learning/vectors/learning/',
        },
      },
    },
  },
  {
    name: 'get_cloudwatch_metrics',
    description: 'Get CloudWatch metrics for a job execution',
    inputSchema: {
      type: 'object',
      properties: {
        namespace: {
          type: 'string',
          description: 'CloudWatch namespace',
          default: 'AWS/Glue',
        },
        metric_name: {
          type: 'string',
          description: 'Metric name (e.g., glue.driver.aggregate.numCompletedTasks)',
        },
        dimensions: {
          type: 'object',
          description: 'Metric dimensions',
        },
        start_time: {
          type: 'string',
          description: 'Start time (ISO 8601)',
        },
        end_time: {
          type: 'string',
          description: 'End time (ISO 8601)',
        },
      },
      required: ['metric_name'],
    },
  },
  {
    name: 'query_issues',
    description: 'Query issues from IssueRegistry table',
    inputSchema: {
      type: 'object',
      properties: {
        issue_type: {
          type: 'string',
          description: 'Type of issue (e.g., multiple_counts, small_files)',
        },
        job_name: {
          type: 'string',
          description: 'Filter by job name',
        },
        severity: {
          type: 'string',
          description: 'Filter by severity',
          enum: ['critical', 'high', 'medium', 'low'],
        },
        limit: {
          type: 'number',
          description: 'Number of issues to return',
          default: 50,
        },
      },
    },
  },
];

// List tools handler
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return { tools };
});

// Call tool handler
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    switch (name) {
      case 'query_execution_history':
        return await queryExecutionHistory(args);

      case 'get_cost_trends':
        return await getCostTrends(args);

      case 'get_quality_trends':
        return await getQualityTrends(args);

      case 'get_glue_job_status':
        return await getGlueJobStatus(args);

      case 'list_learning_vectors':
        return await listLearningVectors(args);

      case 'get_cloudwatch_metrics':
        return await getCloudWatchMetrics(args);

      case 'query_issues':
        return await queryIssues(args);

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `Error: ${error.message}`,
        },
      ],
      isError: true,
    };
  }
});

// Tool implementations

async function queryExecutionHistory({ job_name, limit = 10, status = 'all' }) {
  const params = {
    TableName: 'StrandsJobExecutions',
    IndexName: 'JobNameIndex',
    KeyConditionExpression: 'job_name = :job_name',
    ExpressionAttributeValues: {
      ':job_name': job_name,
    },
    ScanIndexForward: false,
    Limit: limit,
  };

  if (status !== 'all') {
    params.FilterExpression = '#status = :status';
    params.ExpressionAttributeNames = { '#status': 'status' };
    params.ExpressionAttributeValues[':status'] = status;
  }

  const result = await dynamodb.query(params).promise();

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          count: result.Items.length,
          executions: result.Items.map(item => ({
            execution_id: item.execution_id,
            timestamp: item.timestamp,
            status: item.status,
            duration_seconds: item.duration_seconds,
            platform: item.platform_decision?.selected_platform,
            cost: item.cost_breakdown?.total_cost_usd,
            quality_score: item.data_quality?.overall_score,
          })),
        }, null, 2),
      },
    ],
  };
}

async function getCostTrends({ job_name, days = 30 }) {
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);

  const params = {
    TableName: 'StrandsCostTrends',
    KeyConditionExpression: '#date >= :start_date AND job_name = :job_name',
    ExpressionAttributeNames: {
      '#date': 'date',
    },
    ExpressionAttributeValues: {
      ':start_date': startDate.toISOString().split('T')[0],
      ':job_name': job_name,
    },
  };

  const result = await dynamodb.query(params).promise();

  const totalCost = result.Items.reduce((sum, item) => sum + item.total_cost_usd, 0);
  const avgCost = totalCost / result.Items.length;

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          job_name,
          days_analyzed: days,
          total_cost: totalCost.toFixed(2),
          average_daily_cost: avgCost.toFixed(2),
          trend: result.Items,
        }, null, 2),
      },
    ],
  };
}

async function getQualityTrends({ job_name, days = 30 }) {
  const startTime = Date.now() - (days * 24 * 60 * 60 * 1000);

  const params = {
    TableName: 'StrandsDataQualityHistory',
    KeyConditionExpression: 'job_name = :job_name AND #timestamp > :start_time',
    ExpressionAttributeNames: {
      '#timestamp': 'timestamp',
    },
    ExpressionAttributeValues: {
      ':job_name': job_name,
      ':start_time': startTime,
    },
  };

  const result = await dynamodb.query(params).promise();

  const avgQuality = result.Items.reduce((sum, item) => sum + item.overall_score, 0) / result.Items.length;

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          job_name,
          days_analyzed: days,
          average_quality_score: avgQuality.toFixed(3),
          data_points: result.Items.length,
          trend: result.Items.slice(-10).map(item => ({
            date: new Date(item.timestamp).toISOString().split('T')[0],
            overall_score: item.overall_score,
            completeness: item.completeness_score,
            accuracy: item.accuracy_score,
          })),
        }, null, 2),
      },
    ],
  };
}

async function getGlueJobStatus({ job_name, run_id }) {
  const params = { JobName: job_name };
  if (run_id) {
    params.RunId = run_id;
  }

  const result = run_id
    ? await glue.getJobRun(params).promise()
    : await glue.getJobRuns({ JobName: job_name, MaxResults: 1 }).promise();

  const jobRun = run_id ? result.JobRun : result.JobRuns[0];

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          job_name,
          run_id: jobRun.Id,
          status: jobRun.JobRunState,
          started_on: jobRun.StartedOn,
          completed_on: jobRun.CompletedOn,
          execution_time: jobRun.ExecutionTime,
          dpu_seconds: jobRun.DPUSeconds,
        }, null, 2),
      },
    ],
  };
}

async function listLearningVectors({ limit = 100, prefix = 'learning/vectors/learning/' }) {
  const params = {
    Bucket: 'strands-etl-learning',
    Prefix: prefix,
    MaxKeys: limit,
  };

  const result = await s3.listObjectsV2(params).promise();

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          count: result.Contents.length,
          vectors: result.Contents.map(obj => ({
            key: obj.Key,
            size: obj.Size,
            last_modified: obj.LastModified,
          })),
        }, null, 2),
      },
    ],
  };
}

async function getCloudWatchMetrics({ namespace = 'AWS/Glue', metric_name, dimensions = {}, start_time, end_time }) {
  const params = {
    Namespace: namespace,
    MetricName: metric_name,
    Dimensions: Object.entries(dimensions).map(([Name, Value]) => ({ Name, Value })),
    StartTime: start_time ? new Date(start_time) : new Date(Date.now() - 3600000),
    EndTime: end_time ? new Date(end_time) : new Date(),
    Period: 300,
    Statistics: ['Average', 'Maximum', 'Minimum'],
  };

  const result = await cloudwatch.getMetricStatistics(params).promise();

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          metric_name,
          namespace,
          data_points: result.Datapoints.length,
          datapoints: result.Datapoints.sort((a, b) => a.Timestamp - b.Timestamp),
        }, null, 2),
      },
    ],
  };
}

async function queryIssues({ issue_type, job_name, severity, limit = 50 }) {
  let params;

  if (issue_type) {
    params = {
      TableName: 'StrandsIssueRegistry',
      KeyConditionExpression: 'issue_type = :issue_type',
      ExpressionAttributeValues: {
        ':issue_type': issue_type,
      },
      Limit: limit,
    };
  } else if (job_name) {
    params = {
      TableName: 'StrandsIssueRegistry',
      IndexName: 'JobIssuesIndex',
      KeyConditionExpression: 'job_name = :job_name',
      ExpressionAttributeValues: {
        ':job_name': job_name,
      },
      Limit: limit,
    };
  } else {
    params = {
      TableName: 'StrandsIssueRegistry',
      Limit: limit,
    };
  }

  if (severity) {
    const filterExp = params.FilterExpression || '';
    params.FilterExpression = filterExp ? `${filterExp} AND severity = :severity` : 'severity = :severity';
    params.ExpressionAttributeValues[':severity'] = severity;
  }

  const result = issue_type || job_name
    ? await dynamodb.query(params).promise()
    : await dynamodb.scan(params).promise();

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          count: result.Items.length,
          issues: result.Items.map(item => ({
            issue_type: item.issue_type,
            job_name: item.job_name,
            severity: item.severity,
            occurrences: item.occurrences,
            timestamp: item.timestamp,
            resolved: item.resolved,
          })),
        }, null, 2),
      },
    ],
  };
}

// Start server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('Strands AWS MCP Server running on stdio');
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
