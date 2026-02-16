"""
Code Analyzer Agent
===================

Analyzes PySpark code for anti-patterns, optimization opportunities,
and provides specific recommendations for cost reduction.
"""

import re
from typing import Dict, List, Any, Tuple
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult, CodePatternMatcher


class CodeAnalyzerAgent(CostOptimizerAgent):
    """Analyzes PySpark code for optimization opportunities."""

    AGENT_NAME = "code_analyzer"

    # Anti-patterns with severity and cost impact
    ANTI_PATTERNS = {
        'collect_large': {
            'pattern': r'\.collect\(\)',
            'severity': 'critical',
            'cost_impact': 'high',
            'description': 'collect() brings all data to driver - causes OOM on large data',
            'fix': 'Use .take(n), .first(), or write to storage instead'
        },
        'toPandas_large': {
            'pattern': r'\.toPandas\(\)',
            'severity': 'critical',
            'cost_impact': 'high',
            'description': 'toPandas() brings all data to driver memory',
            'fix': 'Process in Spark, or use .limit() before toPandas()'
        },
        'crossJoin': {
            'pattern': r'\.crossJoin\(',
            'severity': 'critical',
            'cost_impact': 'critical',
            'description': 'Cross join creates cartesian product - exponential data explosion',
            'fix': 'Add proper join conditions or filter before join'
        },
        'udf_usage': {
            'pattern': r'@udf|udf\(|\.udf\.',
            'severity': 'high',
            'cost_impact': 'high',
            'description': 'UDFs bypass Catalyst optimizer - 10-100x slower than built-in',
            'fix': 'Replace with Spark SQL functions: concat, when, regexp_extract, etc.'
        },
        'select_star': {
            'pattern': r'\.select\(\s*"\*"\s*\)|\.select\(\s*\'\*\'\s*\)',
            'severity': 'medium',
            'cost_impact': 'medium',
            'description': 'SELECT * reads unnecessary columns',
            'fix': 'Select only needed columns: .select("col1", "col2")'
        },
        'repartition_1': {
            'pattern': r'\.repartition\(1\)',
            'severity': 'high',
            'cost_impact': 'high',
            'description': 'Repartition(1) forces all data to single partition - kills parallelism',
            'fix': 'Use .coalesce() for reducing partitions, or repartition to appropriate number'
        },
        'coalesce_1_before_large_op': {
            'pattern': r'\.coalesce\(1\).*\.(join|groupBy|agg)',
            'severity': 'high',
            'cost_impact': 'high',
            'description': 'Coalesce(1) before join/groupBy kills parallelism',
            'fix': 'Move coalesce to after transformations, before write'
        },
        'count_without_cache': {
            'pattern': r'\.count\(\)[\s\S]{0,200}\.count\(\)',
            'severity': 'medium',
            'cost_impact': 'medium',
            'description': 'Multiple count() calls recompute entire lineage',
            'fix': 'Cache DataFrame before multiple actions: df.cache().count()'
        },
        'show_in_production': {
            'pattern': r'\.show\(',
            'severity': 'low',
            'cost_impact': 'low',
            'description': '.show() triggers action - unnecessary in production',
            'fix': 'Remove .show() calls or guard with debug flag'
        },
        'no_predicate_pushdown': {
            'pattern': r'\.filter\(.*\)[\s\S]*\.read\.',
            'severity': 'medium',
            'cost_impact': 'medium',
            'description': 'Filter after read prevents predicate pushdown',
            'fix': 'Move filter conditions into read: spark.read.filter() or WHERE clause'
        },
        'repeated_read': {
            'pattern': r'spark\.read[\s\S]*?spark\.read',
            'severity': 'medium',
            'cost_impact': 'medium',
            'description': 'Multiple reads of same data source',
            'fix': 'Read once, cache if needed, and reuse DataFrame'
        },
        'string_concat_udf': {
            'pattern': r'lambda.*\+.*str|lambda.*format\(',
            'severity': 'medium',
            'cost_impact': 'medium',
            'description': 'String concatenation in lambda/UDF',
            'fix': 'Use concat() or concat_ws() Spark functions'
        },
        'for_loop_processing': {
            'pattern': r'for\s+\w+\s+in\s+\w+\.collect\(\)',
            'severity': 'critical',
            'cost_impact': 'critical',
            'description': 'Processing data in Python for-loop defeats Spark parallelism',
            'fix': 'Use Spark transformations: map, flatMap, withColumn, etc.'
        },
        'persist_no_unpersist': {
            'pattern': r'\.(cache|persist)\(',
            'needs_validation': 'unpersist',
            'severity': 'low',
            'cost_impact': 'low',
            'description': 'Cache/persist without unpersist may waste memory',
            'fix': 'Add .unpersist() when DataFrame is no longer needed'
        }
    }

    # Spark configurations for optimization
    SPARK_CONFIGS = {
        'shuffle_partitions': {
            'config': 'spark.sql.shuffle.partitions',
            'default': '200',
            'recommendation': 'Set to 2-4x cluster cores for small data, increase for large shuffles',
            'impact': 'Reduces shuffle overhead'
        },
        'adaptive_enabled': {
            'config': 'spark.sql.adaptive.enabled',
            'default': 'false',
            'recommended': 'true',
            'recommendation': 'Enable adaptive query execution for automatic optimization',
            'impact': 'Auto-tunes joins, skew handling, partitions'
        },
        'adaptive_coalesce': {
            'config': 'spark.sql.adaptive.coalescePartitions.enabled',
            'default': 'false',
            'recommended': 'true',
            'recommendation': 'Auto-coalesce small partitions after shuffle',
            'impact': 'Reduces task overhead'
        },
        'adaptive_skew': {
            'config': 'spark.sql.adaptive.skewJoin.enabled',
            'default': 'false',
            'recommended': 'true',
            'recommendation': 'Enable automatic skew join handling',
            'impact': 'Prevents skew-related slow tasks'
        },
        'broadcast_threshold': {
            'config': 'spark.sql.autoBroadcastJoinThreshold',
            'default': '10MB',
            'recommendation': 'Increase to 100MB-500MB if memory allows',
            'impact': 'Avoids shuffle for small table joins'
        },
        'dynamic_allocation': {
            'config': 'spark.dynamicAllocation.enabled',
            'default': 'false',
            'recommended': 'true',
            'recommendation': 'Enable for variable workloads',
            'impact': 'Scale executors based on load'
        }
    }

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """Rule-based code analysis."""

        code = input_data.script_content
        lines = code.split('\n')

        # Detect anti-patterns
        anti_patterns = self._detect_anti_patterns(code, lines)

        # Analyze code complexity
        complexity = self._analyze_complexity(code)

        # Detect optimization opportunities
        optimizations = self._detect_optimizations(code, context)

        # Recommend Spark configs
        spark_configs = self._recommend_spark_configs(code, context)

        # Detect skew mitigations
        skew_mitigations = self._detect_skew_mitigations(code, context)

        # Calculate optimization score
        optimization_score = self._calculate_optimization_score(
            anti_patterns, optimizations, complexity
        )

        # Estimate cost reduction
        cost_reduction = self._estimate_cost_reduction(anti_patterns, optimizations)

        analysis = {
            'anti_patterns': anti_patterns,
            'anti_pattern_count': len(anti_patterns),
            'critical_issues': sum(1 for p in anti_patterns if p['severity'] == 'critical'),
            'complexity': complexity,
            'optimizations': optimizations,
            'spark_configs': spark_configs,
            'skew_mitigations': skew_mitigations,
            'optimization_score': optimization_score,
            'estimated_cost_reduction_percent': cost_reduction,
            'lines_of_code': len(lines),
            'code_quality_score': max(0, 100 - (len(anti_patterns) * 10))
        }

        recommendations = self._generate_recommendations(analysis)

        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis=analysis,
            recommendations=recommendations,
            metrics={
                'anti_pattern_count': len(anti_patterns),
                'critical_issues': analysis['critical_issues'],
                'optimization_score': optimization_score,
                'join_count': complexity['join_count'],
                'window_function_count': complexity['window_function_count']
            }
        )

    def _detect_anti_patterns(self, code: str, lines: List[str]) -> List[Dict]:
        """Detect anti-patterns in code."""
        detected = []

        for name, pattern_info in self.ANTI_PATTERNS.items():
            pattern = pattern_info['pattern']
            matches = []

            for i, line in enumerate(lines, 1):
                if re.search(pattern, line, re.IGNORECASE):
                    matches.append({
                        'line': i,
                        'content': line.strip()[:100]
                    })

            if matches:
                # Check for validation patterns (e.g., cache without unpersist)
                if 'needs_validation' in pattern_info:
                    validation = pattern_info['needs_validation']
                    if validation in code.lower():
                        continue  # Skip if validation pattern found

                detected.append({
                    'pattern': name,
                    'severity': pattern_info['severity'],
                    'cost_impact': pattern_info['cost_impact'],
                    'description': pattern_info['description'],
                    'fix': pattern_info['fix'],
                    'occurrences': matches,
                    'line_numbers': [m['line'] for m in matches]
                })

        return detected

    def _analyze_complexity(self, code: str) -> Dict:
        """Analyze code complexity."""
        return {
            'join_count': CodePatternMatcher.count_joins(code),
            'window_function_count': CodePatternMatcher.count_window_functions(code),
            'aggregation_count': CodePatternMatcher.count_aggregations(code),
            'udf_count': len(re.findall(r'@udf|udf\(', code, re.IGNORECASE)),
            'distinct_count': len(re.findall(r'\.distinct\(', code, re.IGNORECASE)),
            'sort_count': len(re.findall(r'\.sort\(|\.orderBy\(', code, re.IGNORECASE)),
            'union_count': len(re.findall(r'\.union\(|\.unionAll\(', code, re.IGNORECASE)),
            'skew_risk_factors': CodePatternMatcher.detect_skew_risk(code),
            'complexity_score': self._calculate_complexity_score(code)
        }

    def _calculate_complexity_score(self, code: str) -> int:
        """Calculate overall complexity score 0-100."""
        score = 20  # Base score

        # Add complexity for various patterns
        score += CodePatternMatcher.count_joins(code) * 8
        score += CodePatternMatcher.count_window_functions(code) * 10
        score += CodePatternMatcher.count_aggregations(code) * 3
        score += len(re.findall(r'@udf|udf\(', code, re.IGNORECASE)) * 15
        score += len(re.findall(r'\.distinct\(', code, re.IGNORECASE)) * 5
        score += len(re.findall(r'\.sort\(|\.orderBy\(', code, re.IGNORECASE)) * 5

        return min(100, score)

    def _detect_optimizations(self, code: str, context: Dict) -> List[Dict]:
        """Detect optimization opportunities."""
        optimizations = []

        # Check for missing broadcast hints
        join_count = CodePatternMatcher.count_joins(code)
        broadcast_count = len(re.findall(r'broadcast\(', code, re.IGNORECASE))

        if join_count > 0 and broadcast_count == 0:
            optimizations.append({
                'category': 'join_optimization',
                'recommendation': 'Add broadcast hints for small tables',
                'implementation': 'from pyspark.sql.functions import broadcast\ndf.join(broadcast(small_df), "key")',
                'estimated_savings_percent': 15,
                'effort': 'low'
            })

        # Check for missing cache
        action_count = len(re.findall(r'\.(count|collect|show|write|save)\(', code, re.IGNORECASE))
        cache_count = len(re.findall(r'\.(cache|persist)\(', code, re.IGNORECASE))

        if action_count > 1 and cache_count == 0:
            optimizations.append({
                'category': 'caching',
                'recommendation': 'Cache intermediate DataFrames used multiple times',
                'implementation': 'df = df.cache()\n# ... multiple actions ...\ndf.unpersist()',
                'estimated_savings_percent': 30,
                'effort': 'low'
            })

        # Check for predicate pushdown opportunity
        if re.search(r'\.read[\s\S]*?\.filter\(', code, re.IGNORECASE):
            optimizations.append({
                'category': 'io_optimization',
                'recommendation': 'Move filter predicates to read for pushdown',
                'implementation': 'spark.read.parquet(path).filter("date = \'2024-01-01\'")',
                'estimated_savings_percent': 20,
                'effort': 'low'
            })

        # Check for column pruning
        if re.search(r'\.read[\s\S]{0,100}\.select\(', code, re.IGNORECASE):
            pass  # Already has column selection
        elif '.read' in code:
            optimizations.append({
                'category': 'io_optimization',
                'recommendation': 'Select only needed columns early',
                'implementation': 'df = df.select("col1", "col2", "col3")',
                'estimated_savings_percent': 10,
                'effort': 'low'
            })

        # Check for coalesce before write
        if re.search(r'\.write[\s\S]{0,50}\.save|\.write[\s\S]{0,50}\.parquet', code, re.IGNORECASE):
            if not re.search(r'\.coalesce\([^)]+\)[\s\S]{0,50}\.write', code, re.IGNORECASE):
                optimizations.append({
                    'category': 'io_optimization',
                    'recommendation': 'Add coalesce before write to control output files',
                    'implementation': 'df.coalesce(num_files).write.parquet(output_path)',
                    'estimated_savings_percent': 5,
                    'effort': 'low'
                })

        # Check for window function optimization
        window_count = CodePatternMatcher.count_window_functions(code)
        if window_count > 3:
            optimizations.append({
                'category': 'window_optimization',
                'recommendation': 'Consolidate window functions with same partition',
                'implementation': 'Use single window spec for multiple window operations',
                'estimated_savings_percent': 10,
                'effort': 'medium'
            })

        # Check for AQE opportunity
        if 'spark.sql.adaptive.enabled' not in code and join_count > 0:
            optimizations.append({
                'category': 'config_optimization',
                'recommendation': 'Enable Adaptive Query Execution',
                'implementation': '.config("spark.sql.adaptive.enabled", "true")',
                'estimated_savings_percent': 20,
                'effort': 'low'
            })

        return optimizations

    def _recommend_spark_configs(self, code: str, context: Dict) -> List[Dict]:
        """Recommend Spark configurations."""
        configs = []

        # Get context from size analysis
        effective_size_gb = context.get('effective_size_gb', 100)
        join_count = CodePatternMatcher.count_joins(code)

        # Shuffle partitions
        if effective_size_gb < 10:
            shuffle_partitions = 50
        elif effective_size_gb < 100:
            shuffle_partitions = 200
        elif effective_size_gb < 500:
            shuffle_partitions = 400
        else:
            shuffle_partitions = 800

        configs.append({
            'config': 'spark.sql.shuffle.partitions',
            'current_value': '200 (default)',
            'recommended_value': str(shuffle_partitions),
            'reason': f'Based on data size ({effective_size_gb:.0f} GB)'
        })

        # AQE configs
        configs.extend([
            {
                'config': 'spark.sql.adaptive.enabled',
                'current_value': 'false',
                'recommended_value': 'true',
                'reason': 'Enables automatic query optimization'
            },
            {
                'config': 'spark.sql.adaptive.coalescePartitions.enabled',
                'current_value': 'false',
                'recommended_value': 'true',
                'reason': 'Auto-coalesces small partitions'
            },
            {
                'config': 'spark.sql.adaptive.skewJoin.enabled',
                'current_value': 'false',
                'recommended_value': 'true',
                'reason': 'Handles skewed joins automatically'
            }
        ])

        # Broadcast threshold
        if join_count > 0:
            configs.append({
                'config': 'spark.sql.autoBroadcastJoinThreshold',
                'current_value': '10MB',
                'recommended_value': '100MB',
                'reason': 'Increase broadcast threshold to reduce shuffles'
            })

        return configs

    def _detect_skew_mitigations(self, code: str, context: Dict) -> List[Dict]:
        """Detect if skew mitigations are present and recommend if needed."""
        mitigations = []

        skew_risk = context.get('skew_risk_score', 0)
        join_count = CodePatternMatcher.count_joins(code)

        # Check if salting is used
        has_salting = 'salt' in code.lower() or 'rand()' in code.lower()

        # Check if skew hint is used
        has_skew_hint = 'skew' in code.lower()

        if skew_risk > 50 and not has_salting:
            mitigations.append({
                'technique': 'Key Salting',
                'applicable': True,
                'implemented': False,
                'implementation': '''
# Add salt to skewed key
from pyspark.sql.functions import concat, lit, floor, rand

num_salt_buckets = 10
df_large = df_large.withColumn("salt", floor(rand() * num_salt_buckets))
df_large = df_large.withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

# Explode small table
df_small_exploded = df_small.crossJoin(
    spark.range(num_salt_buckets).withColumnRenamed("id", "salt")
)
df_small_exploded = df_small_exploded.withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# Join on salted key
result = df_large.join(df_small_exploded, "salted_key")
''',
                'estimated_improvement': '50-80% for skewed joins'
            })

        if join_count > 3 and not has_skew_hint:
            mitigations.append({
                'technique': 'Skew Join Hint',
                'applicable': True,
                'implemented': False,
                'implementation': '''
# Enable skew join optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
''',
                'estimated_improvement': '30-50% for skewed joins'
            })

        return mitigations

    def _calculate_optimization_score(self, anti_patterns: List, optimizations: List, complexity: Dict) -> int:
        """Calculate overall optimization score 0-100."""
        score = 100

        # Deduct for anti-patterns
        for pattern in anti_patterns:
            if pattern['severity'] == 'critical':
                score -= 20
            elif pattern['severity'] == 'high':
                score -= 10
            elif pattern['severity'] == 'medium':
                score -= 5
            else:
                score -= 2

        # Deduct for missed optimizations
        score -= len(optimizations) * 3

        # Deduct for high complexity without mitigations
        if complexity['complexity_score'] > 70:
            score -= 10

        return max(0, score)

    def _estimate_cost_reduction(self, anti_patterns: List, optimizations: List) -> int:
        """Estimate potential cost reduction from fixes."""
        reduction = 0

        # Anti-pattern fixes
        impact_map = {'critical': 25, 'high': 15, 'medium': 8, 'low': 3}
        for pattern in anti_patterns:
            reduction += impact_map.get(pattern['cost_impact'], 5)

        # Optimization opportunities
        for opt in optimizations:
            reduction += opt.get('estimated_savings_percent', 5)

        # Cap at 80% - can't reduce to zero
        return min(80, reduction)

    def _generate_recommendations(self, analysis: Dict) -> List[Dict]:
        """Generate prioritized recommendations."""
        recommendations = []

        # Critical anti-patterns first
        for pattern in analysis['anti_patterns']:
            if pattern['severity'] in ('critical', 'high'):
                recommendations.append({
                    'priority': 'P0' if pattern['severity'] == 'critical' else 'P1',
                    'category': 'code',
                    'title': f"Fix: {pattern['pattern'].replace('_', ' ').title()}",
                    'description': pattern['description'],
                    'implementation': pattern['fix'],
                    'lines': pattern['line_numbers'],
                    'estimated_savings_percent': 15 if pattern['severity'] == 'critical' else 10
                })

        # Optimization opportunities
        for opt in analysis['optimizations']:
            recommendations.append({
                'priority': 'P1' if opt['effort'] == 'low' else 'P2',
                'category': opt['category'],
                'title': opt['recommendation'],
                'implementation': opt['implementation'],
                'estimated_savings_percent': opt['estimated_savings_percent']
            })

        # Spark config recommendations
        if analysis['spark_configs']:
            recommendations.append({
                'priority': 'P0',
                'category': 'config',
                'title': 'Apply Optimal Spark Configurations',
                'description': 'Update Spark configurations for better performance',
                'configs': analysis['spark_configs'],
                'estimated_savings_percent': 20
            })

        return recommendations
