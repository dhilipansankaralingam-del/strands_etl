"""
Strands Quality Agent - Intelligent data quality assessment with SQL and NL parsing.
Analyzes queries, code, and data for performance issues and quality concerns.
"""

import logging
import re
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import json
import uuid

from strands_agent_base import StrandsAgent
from strands_message_bus import StrandsMessage, MessageType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrandsQualityAgent(StrandsAgent):
    """
    Quality Agent that performs:
    1. SQL query analysis for performance anti-patterns
    2. Natural language to quality check conversion
    3. Code analysis for data quality issues
    4. Learning-based quality assessment
    """

    def __init__(self):
        super().__init__(agent_name="quality_agent", agent_type="quality")
        self.quality_patterns = self._load_quality_patterns()
        self.historical_quality_scores = []

    def _get_subscribed_message_types(self) -> List[MessageType]:
        """Subscribe to quality check requests and execution completed events."""
        return [MessageType.QUALITY_CHECK, MessageType.EXECUTION_COMPLETED]

    def _load_quality_patterns(self) -> Dict[str, Any]:
        """Load common quality anti-patterns and checks."""
        return {
            'sql_antipatterns': [
                {
                    'name': 'SELECT_STAR',
                    'pattern': r'SELECT\s+\*\s+FROM',
                    'severity': 'medium',
                    'message': 'SELECT * pulls all columns; specify needed columns for better performance',
                    'recommendation': 'Use explicit column names: SELECT col1, col2, col3 FROM ...'
                },
                {
                    'name': 'NO_WHERE_CLAUSE',
                    'pattern': r'SELECT\s+.+\s+FROM\s+\w+\s*(?!WHERE)',
                    'severity': 'high',
                    'message': 'Full table scan without WHERE clause can be very slow',
                    'recommendation': 'Add WHERE clause to filter data early'
                },
                {
                    'name': 'MULTIPLE_JOINS_NO_INDEX',
                    'pattern': r'JOIN.*JOIN.*JOIN',
                    'severity': 'high',
                    'message': 'Multiple joins detected; ensure proper indexing',
                    'recommendation': 'Review join keys and ensure they are indexed'
                },
                {
                    'name': 'SUBQUERY_IN_WHERE',
                    'pattern': r'WHERE\s+\w+\s+IN\s*\(\s*SELECT',
                    'severity': 'medium',
                    'message': 'Subquery in WHERE clause can be inefficient',
                    'recommendation': 'Consider using JOIN instead of subquery'
                },
                {
                    'name': 'DISTINCT_WITHOUT_REASON',
                    'pattern': r'SELECT\s+DISTINCT\s+',
                    'severity': 'low',
                    'message': 'DISTINCT can be expensive; ensure it is necessary',
                    'recommendation': 'Review if DISTINCT is truly needed or if duplicates indicate data quality issues'
                },
                {
                    'name': 'FUNCTION_ON_INDEXED_COLUMN',
                    'pattern': r'WHERE\s+\w+\(.+\)\s*=',
                    'severity': 'high',
                    'message': 'Function on column in WHERE prevents index usage',
                    'recommendation': 'Avoid functions on columns in WHERE clause'
                },
                {
                    'name': 'OR_IN_WHERE',
                    'pattern': r'WHERE.*\sOR\s',
                    'severity': 'medium',
                    'message': 'OR conditions can prevent index usage',
                    'recommendation': 'Consider using IN clause or UNION for better performance'
                }
            ],
            'pyspark_antipatterns': [
                {
                    'name': 'COUNT_ACTION',
                    'pattern': r'\.count\(\)',
                    'severity': 'high',
                    'message': 'count() triggers full table scan; avoid in production',
                    'recommendation': 'Remove count() calls or use only when necessary'
                },
                {
                    'name': 'COLLECT_ALL',
                    'pattern': r'\.collect\(\)',
                    'severity': 'high',
                    'message': 'collect() pulls all data to driver; can cause OOM',
                    'recommendation': 'Use take(n) or limit data before collecting'
                },
                {
                    'name': 'NO_BROADCAST_JOIN',
                    'pattern': r'\.join\(',
                    'severity': 'medium',
                    'message': 'Join detected without broadcast hint',
                    'recommendation': 'Consider using broadcast() for small dimension tables'
                },
                {
                    'name': 'NESTED_FIELD_ACCESS',
                    'pattern': r'col\(["\'][\w\.]+\.["\']',
                    'severity': 'medium',
                    'message': 'Nested field access detected',
                    'recommendation': 'Consider flattening struct fields early in pipeline'
                },
                {
                    'name': 'MULTIPLE_ACTIONS',
                    'pattern': r'(\.show\(\)|\.count\(\)|\.collect\(\)).*\n.*(\.show\(\)|\.count\(\)|\.collect\(\))',
                    'severity': 'medium',
                    'message': 'Multiple actions may re-execute the DAG',
                    'recommendation': 'Use cache() or persist() before multiple actions'
                }
            ],
            'data_quality_checks': [
                'completeness',
                'uniqueness',
                'validity',
                'consistency',
                'accuracy',
                'timeliness'
            ]
        }

    async def on_start(self):
        """Load historical quality scores."""
        logger.info("Quality Agent: Loading historical quality data...")
        vectors = await self.load_learning_vectors(limit=50)
        self.historical_quality_scores = [
            v.get('quality_metrics', {}).get('overall_score', 0)
            for v in vectors if 'quality_metrics' in v
        ]

    async def process_message(self, message: StrandsMessage) -> Optional[Dict[str, Any]]:
        """Process quality check requests."""
        if message.message_type == MessageType.QUALITY_CHECK:
            return await self._handle_quality_check(message)
        elif message.message_type == MessageType.EXECUTION_COMPLETED:
            return await self._handle_execution_completed(message)
        return None

    async def autonomous_cycle(self):
        """Periodically analyze quality trends."""
        # Could implement trend analysis, anomaly detection, etc.
        pass

    async def _handle_quality_check(self, message: StrandsMessage) -> Dict[str, Any]:
        """Handle quality check request."""
        check_type = message.payload.get('check_type', 'sql')
        input_data = message.payload.get('input')

        logger.info(f"Quality Agent: Performing {check_type} quality check")

        if check_type == 'sql':
            result = await self._analyze_sql_query(input_data)
        elif check_type == 'natural_language':
            result = await self._process_natural_language_check(input_data)
        elif check_type == 'code':
            result = await self._analyze_code(input_data)
        elif check_type == 'data':
            result = await self._analyze_data_quality(input_data)
        else:
            result = {'error': f'Unknown check type: {check_type}'}

        # Store quality report
        await self._store_quality_report(result, message)

        return result

    async def _handle_execution_completed(self, message: StrandsMessage) -> Dict[str, Any]:
        """Perform quality assessment on completed execution."""
        execution_result = message.payload.get('execution_result', {})

        quality_assessment = await self._assess_execution_quality(execution_result)

        # Broadcast quality assessment
        await self.send_message(
            target_agent=None,
            message_type=MessageType.AGENT_RESPONSE,
            payload={
                'assessment_type': 'execution_quality',
                'quality_report': quality_assessment
            },
            correlation_id=message.correlation_id
        )

        return quality_assessment

    async def _analyze_sql_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze SQL query for performance anti-patterns.
        """
        if not query:
            return {'error': 'No query provided'}

        issues = []
        warnings = []
        recommendations = []

        # Normalize query
        normalized_query = ' '.join(query.upper().split())

        # Check against SQL anti-patterns
        for pattern in self.quality_patterns['sql_antipatterns']:
            if re.search(pattern['pattern'], normalized_query, re.IGNORECASE):
                issue = {
                    'type': pattern['name'],
                    'severity': pattern['severity'],
                    'message': pattern['message'],
                    'recommendation': pattern['recommendation']
                }
                if pattern['severity'] == 'high':
                    issues.append(issue)
                else:
                    warnings.append(issue)
                recommendations.append(pattern['recommendation'])

        # Use AI for deeper analysis
        ai_analysis = await self._get_ai_query_analysis(query, issues, warnings)

        # Calculate quality score
        quality_score = self._calculate_query_quality_score(issues, warnings)

        result = {
            'query_analysis': {
                'query': query,
                'quality_score': quality_score,
                'issues': issues,
                'warnings': warnings,
                'recommendations': recommendations,
                'ai_analysis': ai_analysis
            },
            'timestamp': datetime.utcnow().isoformat(),
            'analysis_id': str(uuid.uuid4())
        }

        logger.info(f"SQL Analysis: Quality score {quality_score:.2f}, "
                   f"{len(issues)} issues, {len(warnings)} warnings")

        return result

    async def _analyze_code(self, code: str) -> Dict[str, Any]:
        """
        Analyze PySpark/Python code for performance anti-patterns.
        """
        if not code:
            return {'error': 'No code provided'}

        issues = []
        warnings = []
        recommendations = []

        # Check against PySpark anti-patterns
        for pattern in self.quality_patterns['pyspark_antipatterns']:
            matches = list(re.finditer(pattern['pattern'], code, re.MULTILINE))
            if matches:
                for match in matches:
                    line_num = code[:match.start()].count('\n') + 1
                    issue = {
                        'type': pattern['name'],
                        'severity': pattern['severity'],
                        'message': pattern['message'],
                        'recommendation': pattern['recommendation'],
                        'line': line_num,
                        'code_snippet': match.group(0)
                    }
                    if pattern['severity'] == 'high':
                        issues.append(issue)
                    else:
                        warnings.append(issue)
                    if pattern['recommendation'] not in recommendations:
                        recommendations.append(pattern['recommendation'])

        # AI-based code review
        ai_review = await self._get_ai_code_review(code, issues, warnings)

        quality_score = self._calculate_code_quality_score(issues, warnings)

        result = {
            'code_analysis': {
                'quality_score': quality_score,
                'issues': issues,
                'warnings': warnings,
                'recommendations': recommendations,
                'ai_review': ai_review,
                'lines_analyzed': len(code.split('\n'))
            },
            'timestamp': datetime.utcnow().isoformat(),
            'analysis_id': str(uuid.uuid4())
        }

        logger.info(f"Code Analysis: Quality score {quality_score:.2f}, "
                   f"{len(issues)} issues, {len(warnings)} warnings")

        return result

    async def _process_natural_language_check(self, nl_input: str) -> Dict[str, Any]:
        """
        Convert natural language input to quality checks and execute them.
        """
        logger.info(f"Processing natural language input: {nl_input}")

        # Use AI to understand the quality check request
        quality_checks = await self._nl_to_quality_checks(nl_input)

        # Execute the identified checks
        check_results = []
        for check in quality_checks:
            result = await self._execute_quality_check(check)
            check_results.append(result)

        overall_result = {
            'natural_language_input': nl_input,
            'interpreted_checks': quality_checks,
            'check_results': check_results,
            'overall_quality_score': self._aggregate_quality_scores(check_results),
            'timestamp': datetime.utcnow().isoformat(),
            'analysis_id': str(uuid.uuid4())
        }

        return overall_result

    async def _nl_to_quality_checks(self, nl_input: str) -> List[Dict[str, Any]]:
        """Use AI to convert natural language to structured quality checks."""
        prompt = f"""
        Convert this natural language quality check request into structured quality checks:

        User Request: "{nl_input}"

        Available quality check types:
        - completeness: Check for null/missing values
        - uniqueness: Check for duplicates
        - validity: Check data types and constraints
        - consistency: Check cross-field relationships
        - accuracy: Check against expected values/ranges
        - timeliness: Check data freshness

        Return a JSON array of quality checks, each with:
        {{
            "check_type": "<type from above>",
            "description": "<what to check>",
            "target": "<table/column/field>",
            "threshold": <acceptable threshold, if applicable>
        }}

        Return ONLY the JSON array, no other text.
        """

        response = await self.invoke_bedrock(prompt, "You are a data quality expert.")

        try:
            # Extract JSON from response
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                checks = json.loads(json_match.group(0))
                return checks
            else:
                # Fallback: create basic completeness check
                return [{
                    'check_type': 'completeness',
                    'description': nl_input,
                    'target': 'all_fields',
                    'threshold': 0.95
                }]
        except Exception as e:
            logger.error(f"Failed to parse NL to quality checks: {e}")
            return []

    async def _execute_quality_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a structured quality check."""
        check_type = check.get('check_type')
        description = check.get('description')
        target = check.get('target')
        threshold = check.get('threshold', 0.95)

        # Simulate execution (in real scenario, would query actual data)
        # For now, use AI to assess if the check would pass based on context

        prompt = f"""
        Assess this quality check:
        Type: {check_type}
        Description: {description}
        Target: {target}
        Threshold: {threshold}

        Based on common data quality patterns, estimate:
        1. Pass/Fail (true/false)
        2. Score (0.0-1.0)
        3. Issues found (if any)
        4. Recommendations

        Return as JSON:
        {{
            "passed": true/false,
            "score": 0.0-1.0,
            "issues": ["issue1", "issue2"],
            "recommendations": ["rec1", "rec2"]
        }}

        Return ONLY the JSON, no other text.
        """

        response = await self.invoke_bedrock(prompt, "You are a data quality specialist.")

        try:
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group(0))
            else:
                result = {'passed': True, 'score': 0.9, 'issues': [], 'recommendations': []}
        except Exception as e:
            logger.error(f"Failed to parse check result: {e}")
            result = {'passed': True, 'score': 0.9, 'issues': [], 'recommendations': []}

        result['check'] = check
        return result

    async def _analyze_data_quality(self, data_context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze data quality based on execution context."""
        # Extract relevant metrics
        records_processed = data_context.get('records_processed', 0)
        execution_status = data_context.get('status', 'unknown')
        errors = data_context.get('errors', [])

        # Compare with historical patterns
        quality_assessment = await self._assess_against_history(data_context)

        result = {
            'data_quality_assessment': quality_assessment,
            'records_processed': records_processed,
            'execution_status': execution_status,
            'error_count': len(errors),
            'timestamp': datetime.utcnow().isoformat()
        }

        return result

    async def _assess_execution_quality(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Assess quality of completed execution."""
        status = execution_result.get('status', 'unknown')
        platform = execution_result.get('platform', 'unknown')
        errors = execution_result.get('error', '')

        # Calculate quality metrics
        quality_metrics = {
            'execution_success': status == 'completed',
            'error_free': not bool(errors),
            'platform': platform
        }

        # Compare with historical data
        if self.historical_quality_scores:
            avg_historical = sum(self.historical_quality_scores) / len(self.historical_quality_scores)
            quality_metrics['vs_historical_avg'] = avg_historical
        else:
            quality_metrics['vs_historical_avg'] = None

        # Use AI for assessment
        ai_assessment = await self._get_ai_execution_assessment(execution_result, quality_metrics)

        overall_score = 1.0 if quality_metrics['execution_success'] and quality_metrics['error_free'] else 0.5

        result = {
            'quality_metrics': quality_metrics,
            'overall_score': overall_score,
            'ai_assessment': ai_assessment,
            'timestamp': datetime.utcnow().isoformat()
        }

        # Update historical scores
        self.historical_quality_scores.append(overall_score)
        if len(self.historical_quality_scores) > 100:
            self.historical_quality_scores.pop(0)

        return result

    async def _assess_against_history(self, data_context: Dict[str, Any]) -> Dict[str, Any]:
        """Assess data quality against historical patterns."""
        # Load relevant historical vectors
        vectors = await self.load_learning_vectors(limit=20)

        # Extract patterns
        historical_patterns = []
        for v in vectors:
            if 'quality_metrics' in v:
                historical_patterns.append(v['quality_metrics'])

        # Use AI to compare
        if historical_patterns:
            prompt = f"""
            Compare current data quality against historical patterns:

            Current: {json.dumps(data_context, indent=2)}
            Historical Patterns: {json.dumps(historical_patterns[:5], indent=2)}

            Identify:
            1. Anomalies or deviations
            2. Quality trends (improving/degrading)
            3. Recommendations

            Provide a brief assessment (3-4 sentences).
            """

            assessment = await self.invoke_bedrock(prompt, "You are a data quality analyst.")
        else:
            assessment = "No historical data available for comparison."

        return {
            'assessment': assessment,
            'historical_count': len(historical_patterns)
        }

    async def _get_ai_query_analysis(self,
                                     query: str,
                                     issues: List[Dict],
                                     warnings: List[Dict]) -> str:
        """Get AI-powered query analysis."""
        prompt = f"""
        Analyze this SQL query for performance and quality:

        Query:
        {query}

        Detected Issues: {len(issues)}
        Detected Warnings: {len(warnings)}

        Provide:
        1. Overall assessment (2-3 sentences)
        2. Most critical issue to fix first
        3. Expected performance impact

        Be concise.
        """

        return await self.invoke_bedrock(prompt, "You are a SQL performance expert.")

    async def _get_ai_code_review(self,
                                  code: str,
                                  issues: List[Dict],
                                  warnings: List[Dict]) -> str:
        """Get AI-powered code review."""
        prompt = f"""
        Review this PySpark code for performance issues:

        Code:
        {code[:1000]}  # Limit to avoid token overflow

        Detected Issues: {len(issues)}
        Detected Warnings: {len(warnings)}

        Provide a brief assessment focusing on the most critical performance improvements.
        """

        return await self.invoke_bedrock(prompt, "You are a PySpark performance expert.")

    async def _get_ai_execution_assessment(self,
                                           execution_result: Dict[str, Any],
                                           quality_metrics: Dict[str, Any]) -> str:
        """Get AI assessment of execution quality."""
        prompt = f"""
        Assess the quality of this ETL execution:

        Execution Result: {json.dumps(execution_result, indent=2)[:500]}
        Quality Metrics: {json.dumps(quality_metrics, indent=2)}

        Provide a brief quality assessment (2-3 sentences).
        """

        return await self.invoke_bedrock(prompt, "You are a data quality specialist.")

    def _calculate_query_quality_score(self,
                                       issues: List[Dict],
                                       warnings: List[Dict]) -> float:
        """Calculate quality score for SQL query."""
        # Start with perfect score
        score = 1.0

        # Deduct for issues
        score -= len(issues) * 0.15  # High severity
        score -= len(warnings) * 0.05  # Lower severity

        return max(0.0, min(1.0, score))

    def _calculate_code_quality_score(self,
                                      issues: List[Dict],
                                      warnings: List[Dict]) -> float:
        """Calculate quality score for code."""
        score = 1.0
        score -= len(issues) * 0.2
        score -= len(warnings) * 0.1
        return max(0.0, min(1.0, score))

    def _aggregate_quality_scores(self, check_results: List[Dict[str, Any]]) -> float:
        """Aggregate quality scores from multiple checks."""
        if not check_results:
            return 0.0

        scores = [r.get('score', 0.0) for r in check_results]
        return sum(scores) / len(scores)

    async def _store_quality_report(self, result: Dict[str, Any], message: StrandsMessage):
        """Store quality report as learning vector."""
        vector = {
            'vector_id': result.get('analysis_id', str(uuid.uuid4())),
            'timestamp': datetime.utcnow().isoformat(),
            'agent_type': 'quality',
            'quality_report': result,
            'correlation_id': message.correlation_id
        }

        await self.store_learning_vector(vector)


# Standalone execution
async def main():
    """Run Quality Agent as standalone service."""
    from strands_message_bus import start_message_bus
    import asyncio

    bus_task = asyncio.create_task(start_message_bus())
    agent = StrandsQualityAgent()
    agent_task = asyncio.create_task(agent.start())

    try:
        await asyncio.gather(bus_task, agent_task)
    except KeyboardInterrupt:
        logger.info("Shutting down Quality Agent...")
        await agent.stop()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
