"""
Code Analysis Agent
===================
Analyzes PySpark code and provides optimization recommendations.

Features:
- Performance optimization suggestions
- Memory optimization
- Shuffle reduction
- Join optimization
- Delta/Iceberg recommendations
- AWS service recommendations
"""

import json
import re
import ast
import logging
import boto3
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationType(Enum):
    """Types of code recommendations."""
    PERFORMANCE = "performance"
    MEMORY = "memory_optimization"
    SHUFFLE = "shuffle_reduction"
    JOIN = "join_optimization"
    PARTITION = "partition_strategy"
    CACHING = "caching_opportunities"
    BROADCAST = "broadcast_opportunities"
    DELTA = "delta_operations"
    SKEW = "data_skew"
    SERIALIZATION = "serialization"
    AWS_SERVICES = "aws_service_recommendations"


class CodeAnalysisAgent:
    """
    Agent for analyzing PySpark code and providing optimization recommendations.
    """

    # Patterns for code analysis
    CODE_PATTERNS = {
        'collect': r'\.collect\(\)',
        'count_in_loop': r'for.*:.*\.count\(\)',
        'udf_usage': r'@udf|udf\(',
        'pandas_udf': r'@pandas_udf|pandas_udf\(',
        'repartition': r'\.repartition\(',
        'coalesce': r'\.coalesce\(',
        'cache': r'\.cache\(\)|\.persist\(',
        'broadcast': r'broadcast\(',
        'join': r'\.join\(',
        'crossjoin': r'\.crossJoin\(',
        'groupby': r'\.groupBy\(',
        'window': r'Window\.',
        'orderby': r'\.orderBy\(|\.sort\(',
        'distinct': r'\.distinct\(',
        'explode': r'explode\(',
        'collect_list': r'collect_list\(',
        'collect_set': r'collect_set\(',
        'toPandas': r'\.toPandas\(\)',
        'show': r'\.show\(',
        'write_csv': r'\.csv\(|format\(["\']csv["\']\)',
        'write_parquet': r'\.parquet\(|format\(["\']parquet["\']\)',
        'write_delta': r'format\(["\']delta["\']\)|\.delta\.',
        'shuffle_partitions': r'spark\.sql\.shuffle\.partitions',
        'broadcast_threshold': r'spark\.sql\.autoBroadcastJoinThreshold'
    }

    # Anti-patterns and their recommendations
    ANTI_PATTERNS = {
        'collect': {
            'severity': 'high',
            'issue': 'Using collect() brings all data to driver, causing OOM for large datasets',
            'recommendation': 'Use take(n), head(n), or write results to storage instead of collect()',
            'example_fix': 'df.write.parquet("output/") instead of df.collect()'
        },
        'toPandas': {
            'severity': 'high',
            'issue': 'toPandas() brings all data to driver memory',
            'recommendation': 'Use Arrow-optimized toPandas with small datasets or use Spark operations',
            'example_fix': 'spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")'
        },
        'crossjoin': {
            'severity': 'critical',
            'issue': 'Cross join creates cartesian product, exponentially increasing data',
            'recommendation': 'Avoid cross joins; use proper join conditions or window functions',
            'example_fix': 'Use explicit join conditions: df1.join(df2, df1.key == df2.key)'
        },
        'show': {
            'severity': 'low',
            'issue': 'show() in production code causes unnecessary computation',
            'recommendation': 'Remove show() calls in production or use logging instead',
            'example_fix': 'Remove df.show() or wrap in if DEBUG: block'
        },
        'udf_usage': {
            'severity': 'medium',
            'issue': 'Python UDFs are slow due to serialization overhead',
            'recommendation': 'Use built-in Spark SQL functions or Pandas UDFs for better performance',
            'example_fix': 'Replace @udf with @pandas_udf or use F.when(), F.coalesce(), etc.'
        }
    }

    # Join optimization patterns
    JOIN_OPTIMIZATIONS = {
        'small_table_broadcast': {
            'condition': 'One table significantly smaller than the other',
            'recommendation': 'Use broadcast hint for smaller table',
            'code': 'df1.join(broadcast(df2), "key")'
        },
        'skewed_join': {
            'condition': 'Join keys have skewed distribution',
            'recommendation': 'Use salting technique or AQE skew join handling',
            'code': '''# Salting technique
df1 = df1.withColumn("salt", F.expr("explode(array(0,1,2,3,4))"))
df2 = df2.withColumn("salt", F.expr("floor(rand() * 5)"))
result = df1.join(df2, ["key", "salt"])'''
        },
        'multiple_joins': {
            'condition': 'Multiple consecutive joins',
            'recommendation': 'Reorder joins - join smaller tables first, cache intermediate results',
            'code': 'small_joined = small1.join(small2, "key").cache()\nresult = large.join(small_joined, "key")'
        }
    }

    def __init__(self, config: Dict[str, Any], region: str = None):
        """
        Initialize Code Analysis Agent.

        Args:
            config: Pipeline configuration
            region: AWS region
        """
        self.config = config
        self.region = region
        self.analysis_config = config.get('code_analysis', {})

        client_kwargs = {'region_name': region} if region else {}
        self.bedrock = boto3.client('bedrock-runtime', **client_kwargs)
        self.s3 = boto3.client('s3', **client_kwargs)

        self.findings = []
        self.recommendations = []

    def is_enabled(self) -> bool:
        """Check if code analysis is enabled."""
        return self.analysis_config.get('enabled', True)

    def analyze_code(self, code: str, script_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze PySpark code and generate recommendations.

        Args:
            code: The PySpark code to analyze
            script_path: Optional path to the script for reference

        Returns:
            Analysis results with recommendations
        """
        if not self.is_enabled():
            return {'enabled': False, 'message': 'Code analysis is disabled'}

        results = {
            'analyzed_at': datetime.utcnow().isoformat(),
            'script_path': script_path,
            'code_length': len(code),
            'line_count': code.count('\n') + 1,
            'pattern_findings': [],
            'anti_patterns': [],
            'join_analysis': [],
            'optimization_opportunities': [],
            'aws_recommendations': [],
            'delta_recommendations': [],
            'overall_score': 100,
            'risk_areas': []
        }

        # Pattern matching analysis
        results['pattern_findings'] = self._find_patterns(code)

        # Anti-pattern detection
        results['anti_patterns'] = self._detect_anti_patterns(code)

        # Join analysis
        results['join_analysis'] = self._analyze_joins(code)

        # Optimization opportunities
        results['optimization_opportunities'] = self._find_optimizations(code)

        # AWS service recommendations
        results['aws_recommendations'] = self._generate_aws_recommendations(code, results)

        # Delta/Iceberg recommendations
        results['delta_recommendations'] = self._generate_delta_recommendations(code)

        # Calculate overall score
        results['overall_score'] = self._calculate_score(results)

        # Identify risk areas
        results['risk_areas'] = self._identify_risks(results)

        # AI-powered deep analysis
        if self.analysis_config.get('analyze_categories', []):
            ai_analysis = self._ai_deep_analysis(code, results)
            results['ai_analysis'] = ai_analysis

        self.findings.append(results)
        return results

    def _find_patterns(self, code: str) -> List[Dict[str, Any]]:
        """Find code patterns in the script."""
        findings = []
        for pattern_name, pattern in self.CODE_PATTERNS.items():
            matches = list(re.finditer(pattern, code, re.IGNORECASE))
            if matches:
                findings.append({
                    'pattern': pattern_name,
                    'count': len(matches),
                    'locations': [m.start() for m in matches[:5]]  # First 5 locations
                })
        return findings

    def _detect_anti_patterns(self, code: str) -> List[Dict[str, Any]]:
        """Detect anti-patterns in the code."""
        anti_patterns = []
        for pattern_name, pattern in self.CODE_PATTERNS.items():
            if pattern_name in self.ANTI_PATTERNS:
                if re.search(pattern, code, re.IGNORECASE):
                    anti_pattern_info = self.ANTI_PATTERNS[pattern_name].copy()
                    anti_pattern_info['pattern'] = pattern_name

                    # Find line numbers
                    lines = code.split('\n')
                    line_numbers = []
                    for i, line in enumerate(lines, 1):
                        if re.search(pattern, line, re.IGNORECASE):
                            line_numbers.append(i)
                    anti_pattern_info['line_numbers'] = line_numbers[:5]

                    anti_patterns.append(anti_pattern_info)

        return anti_patterns

    def _analyze_joins(self, code: str) -> List[Dict[str, Any]]:
        """Analyze join operations in the code."""
        joins = []
        join_pattern = r'\.join\s*\((.*?)\)'
        matches = re.finditer(join_pattern, code, re.DOTALL)

        for match in matches:
            join_info = {
                'location': match.start(),
                'join_code': match.group(0)[:100],
                'recommendations': []
            }

            # Check for broadcast hint
            if 'broadcast' not in match.group(0).lower():
                join_info['recommendations'].append({
                    'type': 'broadcast',
                    'message': 'Consider using broadcast() for smaller table',
                    'code': 'from pyspark.sql.functions import broadcast\ndf1.join(broadcast(df2), "key")'
                })

            # Check for join type
            if 'outer' in match.group(0).lower() or 'full' in match.group(0).lower():
                join_info['recommendations'].append({
                    'type': 'join_type',
                    'message': 'Outer/Full joins are expensive; ensure they are necessary',
                    'suggestion': 'Consider if left join with coalesce would work instead'
                })

            joins.append(join_info)

        return joins

    def _find_optimizations(self, code: str) -> List[Dict[str, Any]]:
        """Find optimization opportunities in the code."""
        optimizations = []

        # Check for missing caching
        if '.join(' in code.lower() and '.cache()' not in code.lower():
            if code.lower().count('.join(') > 1:
                optimizations.append({
                    'type': 'caching',
                    'severity': 'medium',
                    'message': 'Multiple joins detected without caching intermediate results',
                    'recommendation': 'Cache DataFrames that are used multiple times',
                    'code': 'intermediate_df = df1.join(df2, "key").cache()'
                })

        # Check for missing partition strategy
        if '.write' in code.lower() and '.partitionBy(' not in code.lower():
            optimizations.append({
                'type': 'partitioning',
                'severity': 'medium',
                'message': 'Writing data without partitioning',
                'recommendation': 'Partition output data by common query columns',
                'code': 'df.write.partitionBy("date", "region").parquet("output/")'
            })

        # Check for CSV output (recommend Parquet)
        if re.search(self.CODE_PATTERNS['write_csv'], code):
            optimizations.append({
                'type': 'format',
                'severity': 'medium',
                'message': 'Writing to CSV format is slow and lacks compression',
                'recommendation': 'Use Parquet or Delta format for better performance',
                'code': 'df.write.parquet("output/") or df.write.format("delta").save("output/")'
            })

        # Check for missing coalesce before write
        if '.write' in code.lower() and '.coalesce(' not in code.lower():
            optimizations.append({
                'type': 'file_count',
                'severity': 'low',
                'message': 'Writing without coalesce may create many small files',
                'recommendation': 'Use coalesce() to control output file count',
                'code': 'df.coalesce(10).write.parquet("output/")'
            })

        # Check shuffle partition settings
        if 'shuffle.partitions' not in code.lower() and '.join(' in code.lower():
            optimizations.append({
                'type': 'shuffle',
                'severity': 'medium',
                'message': 'Shuffle partitions not configured (default is 200)',
                'recommendation': 'Set shuffle partitions based on data size',
                'code': 'spark.conf.set("spark.sql.shuffle.partitions", "auto") # or specific number'
            })

        # Check for orderBy before write
        if re.search(r'\.orderBy\([^)]+\)\.write', code, re.IGNORECASE):
            optimizations.append({
                'type': 'sorting',
                'severity': 'high',
                'message': 'Global orderBy before write causes full shuffle',
                'recommendation': 'Use sortWithinPartitions or remove if not needed',
                'code': 'df.sortWithinPartitions("column").write.parquet("output/")'
            })

        return optimizations

    def _generate_aws_recommendations(self, code: str, analysis: Dict) -> List[Dict[str, Any]]:
        """Generate AWS service recommendations."""
        recommendations = []

        # Recommend Glue Auto Scaling
        if len(analysis.get('join_analysis', [])) > 2:
            recommendations.append({
                'service': 'AWS Glue Auto Scaling',
                'reason': 'Complex job with multiple joins detected',
                'recommendation': 'Enable Glue Auto Scaling for dynamic resource allocation',
                'implementation': 'Set NumberOfWorkers to "auto" in Glue job configuration'
            })

        # Recommend Glue Data Quality
        if 'assert' not in code.lower() and 'quality' not in code.lower():
            recommendations.append({
                'service': 'AWS Glue Data Quality',
                'reason': 'No data quality checks detected in code',
                'recommendation': 'Use Glue Data Quality for automated quality rules',
                'implementation': 'Add DQDL rules in Glue Studio or use evaluate_data_quality()'
            })

        # Recommend Lake Formation for PII
        if any(p['pattern'] in ['email', 'phone', 'ssn'] for p in analysis.get('pattern_findings', [])):
            recommendations.append({
                'service': 'AWS Lake Formation',
                'reason': 'PII-related columns detected',
                'recommendation': 'Use Lake Formation for column-level security',
                'implementation': 'Configure LF-Tags and column permissions'
            })

        # Recommend Step Functions for orchestration
        recommendations.append({
            'service': 'AWS Step Functions',
            'reason': 'ETL job orchestration',
            'recommendation': 'Use Step Functions for workflow orchestration with error handling',
            'implementation': 'Create state machine with Glue job integration'
        })

        # Recommend CloudWatch for monitoring
        recommendations.append({
            'service': 'Amazon CloudWatch',
            'reason': 'Job monitoring and alerting',
            'recommendation': 'Set up CloudWatch dashboards and alarms for job metrics',
            'implementation': 'Enable continuous logging and create metric filters'
        })

        return recommendations

    def _generate_delta_recommendations(self, code: str) -> List[Dict[str, Any]]:
        """Generate Delta Lake/Iceberg recommendations."""
        recommendations = []

        # Check if already using Delta
        uses_delta = bool(re.search(self.CODE_PATTERNS['write_delta'], code))

        if not uses_delta:
            recommendations.append({
                'type': 'format_upgrade',
                'title': 'Consider Delta Lake Format',
                'reason': 'Delta Lake provides ACID transactions, time travel, and better performance',
                'benefits': [
                    'ACID transactions for reliable writes',
                    'Time travel for data versioning',
                    'Schema enforcement and evolution',
                    'Optimized file management with OPTIMIZE and VACUUM',
                    'Change data capture with CDF'
                ],
                'implementation': '''
# Write as Delta
df.write.format("delta").mode("overwrite").save("s3://bucket/delta_table/")

# Or with merge for upserts
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3://bucket/delta_table/")
delta_table.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
'''
            })

        if uses_delta:
            # Recommend Delta optimizations
            if 'OPTIMIZE' not in code.upper():
                recommendations.append({
                    'type': 'delta_optimize',
                    'title': 'Add Delta OPTIMIZE',
                    'recommendation': 'Run OPTIMIZE periodically to compact small files',
                    'code': 'spark.sql("OPTIMIZE delta.`s3://bucket/delta_table/`")'
                })

            if 'VACUUM' not in code.upper():
                recommendations.append({
                    'type': 'delta_vacuum',
                    'title': 'Add Delta VACUUM',
                    'recommendation': 'Run VACUUM to remove old files and save storage',
                    'code': 'spark.sql("VACUUM delta.`s3://bucket/delta_table/` RETAIN 168 HOURS")'
                })

            if 'Z-ORDER' not in code.upper() and 'ZORDER' not in code.upper():
                recommendations.append({
                    'type': 'delta_zorder',
                    'title': 'Consider Z-ORDER clustering',
                    'recommendation': 'Z-ORDER by frequently filtered columns for faster queries',
                    'code': 'spark.sql("OPTIMIZE delta.`s3://bucket/delta_table/` ZORDER BY (date, customer_id)")'
                })

        return recommendations

    def _calculate_score(self, analysis: Dict) -> int:
        """Calculate overall code quality score (0-100)."""
        score = 100

        # Deduct for anti-patterns
        for ap in analysis.get('anti_patterns', []):
            if ap['severity'] == 'critical':
                score -= 20
            elif ap['severity'] == 'high':
                score -= 10
            elif ap['severity'] == 'medium':
                score -= 5
            else:
                score -= 2

        # Deduct for optimization opportunities
        for opt in analysis.get('optimization_opportunities', []):
            if opt['severity'] == 'high':
                score -= 5
            elif opt['severity'] == 'medium':
                score -= 3
            else:
                score -= 1

        return max(0, score)

    def _identify_risks(self, analysis: Dict) -> List[Dict[str, Any]]:
        """Identify high-risk areas in the code."""
        risks = []

        # Check for critical anti-patterns
        for ap in analysis.get('anti_patterns', []):
            if ap['severity'] in ['critical', 'high']:
                risks.append({
                    'type': 'anti_pattern',
                    'severity': ap['severity'],
                    'description': ap['issue'],
                    'line_numbers': ap.get('line_numbers', [])
                })

        # Check for performance risks
        if len(analysis.get('join_analysis', [])) > 3:
            risks.append({
                'type': 'performance',
                'severity': 'high',
                'description': f"Complex query with {len(analysis['join_analysis'])} joins may cause performance issues"
            })

        return risks

    def _ai_deep_analysis(self, code: str, basic_analysis: Dict) -> Dict[str, Any]:
        """Use AI for deeper code analysis."""
        # Truncate code if too long
        code_sample = code[:8000] if len(code) > 8000 else code

        prompt = f"""
        You are a PySpark performance expert. Analyze this code and provide optimization recommendations.

        CODE:
        ```python
        {code_sample}
        ```

        BASIC ANALYSIS FINDINGS:
        - Anti-patterns found: {len(basic_analysis.get('anti_patterns', []))}
        - Join operations: {len(basic_analysis.get('join_analysis', []))}
        - Optimization opportunities: {len(basic_analysis.get('optimization_opportunities', []))}

        Provide a JSON response with:
        {{
            "code_summary": "Brief description of what the code does",
            "complexity_assessment": "low/medium/high",
            "top_3_optimizations": [
                {{"issue": "...", "fix": "...", "impact": "high/medium/low"}}
            ],
            "spark_config_recommendations": {{
                "setting_name": "recommended_value"
            }},
            "refactoring_suggestions": ["list of suggestions"],
            "estimated_performance_improvement": "percentage estimate"
        }}
        """

        try:
            response = self.bedrock.invoke_model(
                modelId='anthropic.claude-3-sonnet-20240229-v1:0',
                body=json.dumps({
                    'anthropic_version': 'bedrock-2023-05-31',
                    'max_tokens': 2000,
                    'messages': [{'role': 'user', 'content': prompt}]
                })
            )

            result = json.loads(response['body'].read())
            answer = result['content'][0]['text']

            # Parse JSON from response
            if '{' in answer:
                start = answer.find('{')
                end = answer.rfind('}') + 1
                return json.loads(answer[start:end])

        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return {'error': str(e)}

        return {}

    def get_optimized_code(self, code: str) -> str:
        """
        Generate optimized version of the code.

        Args:
            code: Original code

        Returns:
            Optimized code with improvements
        """
        if not self.analysis_config.get('generate_optimized_code', False):
            return code

        # Analyze first
        analysis = self.analyze_code(code)

        # Apply automatic fixes for simple patterns
        optimized = code

        # Add caching hints
        if 'cache()' not in code.lower() and len(analysis.get('join_analysis', [])) > 1:
            optimized = "# Consider adding .cache() for DataFrames used multiple times\n" + optimized

        # Add shuffle partition config
        if 'shuffle.partitions' not in code.lower():
            optimized = '# Set shuffle partitions based on data size\nspark.conf.set("spark.sql.shuffle.partitions", "auto")\n\n' + optimized

        return optimized

    def generate_report(self) -> Dict[str, Any]:
        """Generate a summary report of all analyses."""
        return {
            'total_analyses': len(self.findings),
            'findings': self.findings,
            'common_issues': self._get_common_issues(),
            'overall_recommendations': self._aggregate_recommendations()
        }

    def _get_common_issues(self) -> List[Dict[str, Any]]:
        """Get most common issues across all analyses."""
        issue_counts = {}
        for finding in self.findings:
            for ap in finding.get('anti_patterns', []):
                pattern = ap['pattern']
                issue_counts[pattern] = issue_counts.get(pattern, 0) + 1

        return sorted(
            [{'issue': k, 'count': v} for k, v in issue_counts.items()],
            key=lambda x: x['count'],
            reverse=True
        )

    def _aggregate_recommendations(self) -> List[str]:
        """Aggregate recommendations from all analyses."""
        all_recs = set()
        for finding in self.findings:
            for opt in finding.get('optimization_opportunities', []):
                all_recs.add(opt['recommendation'])
            for aws in finding.get('aws_recommendations', []):
                all_recs.add(f"{aws['service']}: {aws['recommendation']}")

        return list(all_recs)[:10]  # Top 10
