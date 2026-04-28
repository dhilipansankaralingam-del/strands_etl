"""
Size Analyzer Agent
===================

Analyzes source tables to estimate data volumes and processing characteristics.
"""

from typing import Dict, List, Any
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


class SizeAnalyzerAgent(CostOptimizerAgent):
    """Analyzes data sizes and estimates processing requirements."""

    AGENT_NAME = "size_analyzer"

    # Bytes per row estimates by table width
    BYTES_PER_ROW = {
        'narrow': 200,      # < 20 columns
        'medium': 500,      # 20-50 columns
        'wide': 1000,       # 50-100 columns
        'very_wide': 2000   # 100+ columns
    }

    # Compression ratios for different formats
    COMPRESSION_RATIO = {
        'parquet': 0.25,
        'orc': 0.25,
        'delta': 0.25,
        'avro': 0.5,
        'json': 0.7,
        'csv': 0.6
    }

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """Rule-based size analysis."""

        tables_analysis = []
        total_raw_size_bytes = 0
        total_compressed_size_bytes = 0
        skew_risks = []

        for table in input_data.source_tables:
            table_analysis = self._analyze_table(table, input_data.processing_mode)
            tables_analysis.append(table_analysis)

            total_raw_size_bytes += table_analysis['raw_size_bytes']
            total_compressed_size_bytes += table_analysis['compressed_size_bytes']

            if table_analysis.get('skew_risk', 'low') != 'low':
                skew_risks.append({
                    'table': table.get('table', table.get('name', 'unknown')),
                    'risk': table_analysis['skew_risk'],
                    'reason': table_analysis.get('skew_reason', '')
                })

        # Calculate effective size based on processing mode
        if input_data.processing_mode == 'delta':
            delta_ratio = self._estimate_delta_ratio(input_data)
            effective_size_bytes = total_compressed_size_bytes * delta_ratio
        else:
            effective_size_bytes = total_compressed_size_bytes
            delta_ratio = 1.0

        # Calculate join amplification
        join_factor = self._estimate_join_amplification(input_data, context)

        # Final effective size
        final_effective_bytes = effective_size_bytes * join_factor

        # Convert to GB
        total_raw_gb = total_raw_size_bytes / (1024 ** 3)
        total_compressed_gb = total_compressed_size_bytes / (1024 ** 3)
        effective_gb = final_effective_bytes / (1024 ** 3)

        # Calculate skew risk score
        skew_score = self._calculate_skew_score(skew_risks, input_data)

        # Calculate partition efficiency
        partition_efficiency = self._analyze_partitions(input_data.source_tables)

        analysis = {
            'total_raw_size_gb': round(total_raw_gb, 2),
            'total_compressed_size_gb': round(total_compressed_gb, 2),
            'effective_size_gb': round(effective_gb, 2),
            'processing_mode': input_data.processing_mode,
            'delta_ratio': round(delta_ratio, 3),
            'join_amplification_factor': round(join_factor, 2),
            'skew_risk_score': skew_score,
            'skew_risk_factors': skew_risks,
            'partition_efficiency_score': partition_efficiency['score'],
            'partition_recommendations': partition_efficiency['recommendations'],
            'tables_analyzed': len(tables_analysis),
            'tables_detail': tables_analysis,
            'size_confidence': self._calculate_confidence(input_data)
        }

        recommendations = self._generate_recommendations(analysis)

        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis=analysis,
            recommendations=recommendations,
            metrics={
                'total_tables': len(tables_analysis),
                'effective_size_gb': analysis['effective_size_gb'],
                'skew_risk_score': skew_score
            }
        )

    def _analyze_table(self, table: Dict, processing_mode: str) -> Dict:
        """Analyze a single table."""
        record_count = table.get('record_count', table.get('records', 0))
        column_count = table.get('column_count', table.get('columns', 30))
        format_type = table.get('format', 'parquet').lower()
        table_name = table.get('table', table.get('name', 'unknown'))

        # Determine table width category
        if column_count < 20:
            width_category = 'narrow'
        elif column_count < 50:
            width_category = 'medium'
        elif column_count < 100:
            width_category = 'wide'
        else:
            width_category = 'very_wide'

        # Calculate sizes
        bytes_per_row = self.BYTES_PER_ROW[width_category]
        raw_size_bytes = record_count * bytes_per_row

        compression = self.COMPRESSION_RATIO.get(format_type, 0.3)
        compressed_size_bytes = raw_size_bytes * compression

        # Check for provided size override
        if 'size_gb' in table:
            compressed_size_bytes = table['size_gb'] * (1024 ** 3)
            raw_size_bytes = compressed_size_bytes / compression

        # Skew risk analysis
        skew_risk, skew_reason = self._assess_table_skew(table)

        return {
            'table': table_name,
            'database': table.get('database', ''),
            'record_count': record_count,
            'column_count': column_count,
            'format': format_type,
            'width_category': width_category,
            'raw_size_bytes': int(raw_size_bytes),
            'compressed_size_bytes': int(compressed_size_bytes),
            'compressed_size_gb': round(compressed_size_bytes / (1024 ** 3), 2),
            'skew_risk': skew_risk,
            'skew_reason': skew_reason,
            'is_broadcast_candidate': compressed_size_bytes < 100 * 1024 * 1024  # < 100MB
        }

    def _assess_table_skew(self, table: Dict) -> tuple:
        """Assess skew risk for a table."""
        # Check for explicit skew indicators
        if table.get('has_skew', False):
            return 'high', 'Explicitly marked as skewed'

        # Check partition column
        partition_col = table.get('partition_column', '')
        if partition_col.lower() in ['date', 'dt', 'process_date']:
            return 'medium', 'Date partition may have uneven distribution'

        # Check if it's a dimension table (small)
        if table.get('record_count', 0) < 100000:
            return 'low', 'Small table, skew unlikely'

        # Check for known skewed patterns
        table_name = table.get('table', '').lower()
        if any(x in table_name for x in ['transaction', 'event', 'log', 'click']):
            return 'medium', 'Transactional table may have temporal skew'

        return 'low', ''

    def _estimate_delta_ratio(self, input_data: AnalysisInput) -> float:
        """Estimate what fraction of data will be processed in delta mode."""
        # Check for explicit delta ratio
        if 'delta_ratio' in input_data.additional_context:
            return input_data.additional_context['delta_ratio']

        # Default assumptions
        # Daily delta on large tables: ~1-5% of data
        # Hourly delta: ~0.1-0.5%
        # Monthly tables: ~3-10%

        schedule = input_data.additional_context.get('schedule', 'daily')

        if schedule == 'hourly':
            return 0.005
        elif schedule == 'daily':
            return 0.03
        elif schedule == 'weekly':
            return 0.15
        else:
            return 0.05  # Default 5%

    def _estimate_join_amplification(self, input_data: AnalysisInput, context: Dict) -> float:
        """Estimate data amplification from joins."""
        # Get join count from code analysis if available
        join_count = context.get('join_count', 0)

        if join_count == 0:
            # Count joins in script
            import re
            join_count = len(re.findall(r'\.join\(', input_data.script_content, re.IGNORECASE))

        # Estimate amplification
        # Each join can expand or contract data
        # Assumption: average join is 1.2x amplification (some expand, some filter)
        if join_count == 0:
            return 1.0
        elif join_count <= 3:
            return 1.0 + (join_count * 0.1)
        elif join_count <= 10:
            return 1.3 + ((join_count - 3) * 0.05)
        else:
            return 1.5 + ((join_count - 10) * 0.02)

    def _calculate_skew_score(self, skew_risks: List, input_data: AnalysisInput) -> int:
        """Calculate overall skew risk score 0-100."""
        if not skew_risks:
            return 10  # Low baseline risk

        high_risks = sum(1 for r in skew_risks if r['risk'] == 'high')
        medium_risks = sum(1 for r in skew_risks if r['risk'] == 'medium')

        score = 10 + (high_risks * 30) + (medium_risks * 15)
        return min(100, score)

    def _analyze_partitions(self, tables: List[Dict]) -> Dict:
        """Analyze partition efficiency across tables."""
        partition_issues = []
        tables_with_partitions = 0

        for table in tables:
            if table.get('partition_column'):
                tables_with_partitions += 1
            else:
                if table.get('record_count', 0) > 1000000:
                    partition_issues.append(f"Large table {table.get('table')} has no partition")

        if not tables:
            return {'score': 50, 'recommendations': []}

        partition_ratio = tables_with_partitions / len(tables)
        score = int(partition_ratio * 100)

        recommendations = partition_issues[:3]  # Top 3 issues

        return {
            'score': score,
            'tables_with_partitions': tables_with_partitions,
            'total_tables': len(tables),
            'recommendations': recommendations
        }

    def _calculate_confidence(self, input_data: AnalysisInput) -> str:
        """Calculate confidence level in size estimates."""
        # High confidence if we have explicit sizes or record counts
        tables_with_data = sum(
            1 for t in input_data.source_tables
            if t.get('size_gb') or t.get('record_count', 0) > 0
        )

        ratio = tables_with_data / max(len(input_data.source_tables), 1)

        if ratio > 0.8:
            return 'high'
        elif ratio > 0.5:
            return 'medium'
        else:
            return 'low'

    def _generate_recommendations(self, analysis: Dict) -> List[Dict]:
        """Generate recommendations based on analysis."""
        recommendations = []

        # Size-based recommendations
        effective_gb = analysis['effective_size_gb']

        if effective_gb > 500:
            recommendations.append({
                'priority': 'P1',
                'category': 'architecture',
                'title': 'Consider EKS with Karpenter',
                'description': f'Data size ({effective_gb:.0f} GB) exceeds optimal Glue threshold. '
                               'EKS with Karpenter can reduce costs by 60-70% using spot instances.',
                'estimated_savings_percent': 65
            })
        elif effective_gb > 100:
            recommendations.append({
                'priority': 'P2',
                'category': 'resource',
                'title': 'Consider EMR with Spot',
                'description': f'Data size ({effective_gb:.0f} GB) may benefit from EMR with spot instances.',
                'estimated_savings_percent': 40
            })

        # Skew recommendations
        if analysis['skew_risk_score'] > 50:
            recommendations.append({
                'priority': 'P1',
                'category': 'code',
                'title': 'Address Data Skew',
                'description': f'High skew risk detected (score: {analysis["skew_risk_score"]}). '
                               'Implement key salting or skew join hints.',
                'estimated_savings_percent': 20
            })

        # Partition recommendations
        if analysis['partition_efficiency_score'] < 50:
            recommendations.append({
                'priority': 'P2',
                'category': 'architecture',
                'title': 'Improve Partition Strategy',
                'description': 'Several large tables lack partitioning. Add partitions to enable pruning.',
                'estimated_savings_percent': 15
            })

        # Delta mode recommendation
        if analysis['processing_mode'] == 'full' and analysis['total_compressed_size_gb'] > 50:
            recommendations.append({
                'priority': 'P0',
                'category': 'architecture',
                'title': 'Switch to Delta Processing',
                'description': 'Full processing on large dataset. Delta/incremental processing '
                               'could reduce data volume by 90-97%.',
                'estimated_savings_percent': 90
            })

        # Broadcast candidates
        broadcast_candidates = [
            t['table'] for t in analysis['tables_detail']
            if t.get('is_broadcast_candidate', False)
        ]
        if broadcast_candidates:
            recommendations.append({
                'priority': 'P1',
                'category': 'code',
                'title': 'Use Broadcast Joins',
                'description': f'Tables suitable for broadcast: {", ".join(broadcast_candidates[:5])}. '
                               'Add broadcast hints to reduce shuffle.',
                'estimated_savings_percent': 10
            })

        return recommendations
