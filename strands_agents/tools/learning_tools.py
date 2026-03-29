"""
Learning Agent Tools - Pattern recognition and historical analysis
"""
from typing import Dict, Any, List
from strands import tool
import math

@tool
def search_similar_workloads(
    current_workload: Dict[str, Any],
    historical_workloads: List[Dict[str, Any]],
    top_k: int = 5
) -> Dict[str, Any]:
    """
    Find similar historical workloads using cosine similarity.

    Args:
        current_workload: Current workload characteristics
        historical_workloads: List of historical workload records
        top_k: Number of top similar workloads to return

    Returns:
        Most similar workloads with similarity scores
    """
    def extract_features(workload: Dict[str, Any]) -> List[float]:
        """Extract numeric features for comparison"""
        return [
            float(workload.get('data_volume_gb', 0)),
            float(workload.get('file_count', 0)),
            float(workload.get('complexity_score', 0)),
            float(workload.get('duration_minutes', 0))
        ]

    def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(b * b for b in vec2))
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        return dot_product / (magnitude1 * magnitude2)

    current_features = extract_features(current_workload)
    similarities = []

    for hist_workload in historical_workloads:
        hist_features = extract_features(hist_workload)
        similarity = cosine_similarity(current_features, hist_features)

        similarities.append({
            'workload_id': hist_workload.get('id', 'unknown'),
            'similarity_score': round(similarity, 3),
            'platform_used': hist_workload.get('platform', 'unknown'),
            'duration_minutes': hist_workload.get('duration_minutes', 0),
            'cost_usd': hist_workload.get('cost_usd', 0),
            'success': hist_workload.get('success', False)
        })

    # Sort by similarity and return top K
    similarities.sort(key=lambda x: x['similarity_score'], reverse=True)
    top_similar = similarities[:top_k]

    return {
        'similar_workloads_found': len(top_similar),
        'top_matches': top_similar,
        'average_similarity': round(sum(w['similarity_score'] for w in top_similar) / len(top_similar), 3) if top_similar else 0,
        'recommendation': top_similar[0] if top_similar else None
    }


@tool
def analyze_success_patterns(
    execution_history: List[Dict[str, Any]],
    min_confidence: float = 0.7
) -> Dict[str, Any]:
    """
    Analyze patterns in successful vs failed executions.

    Args:
        execution_history: List of past execution records
        min_confidence: Minimum confidence threshold for patterns

    Returns:
        Identified patterns and insights
    """
    if not execution_history:
        return {'error': 'No execution history provided'}

    successful = [e for e in execution_history if e.get('success', False)]
    failed = [e for e in execution_history if not e.get('success', False)]

    success_rate = len(successful) / len(execution_history) if execution_history else 0

    patterns = []

    # Pattern 1: Platform success rates
    platform_stats = {}
    for execution in execution_history:
        platform = execution.get('platform', 'unknown')
        if platform not in platform_stats:
            platform_stats[platform] = {'total': 0, 'successful': 0}
        platform_stats[platform]['total'] += 1
        if execution.get('success', False):
            platform_stats[platform]['successful'] += 1

    for platform, stats in platform_stats.items():
        platform_success_rate = stats['successful'] / stats['total'] if stats['total'] > 0 else 0
        if platform_success_rate >= min_confidence:
            patterns.append({
                'pattern_type': 'platform_reliability',
                'platform': platform,
                'success_rate': round(platform_success_rate, 2),
                'sample_size': stats['total'],
                'confidence': 'high' if stats['total'] >= 10 else 'medium'
            })

    # Pattern 2: Data volume sweet spots
    if successful:
        avg_volume_success = sum(e.get('data_volume_gb', 0) for e in successful) / len(successful)
        avg_volume_failed = sum(e.get('data_volume_gb', 0) for e in failed) / len(failed) if failed else 0

        if abs(avg_volume_success - avg_volume_failed) > 10:
            patterns.append({
                'pattern_type': 'data_volume_correlation',
                'finding': f'Successful jobs avg {avg_volume_success:.1f}GB vs failed {avg_volume_failed:.1f}GB',
                'recommendation': f'Optimal data volume range: {avg_volume_success * 0.8:.1f}-{avg_volume_success * 1.2:.1f}GB'
            })

    return {
        'total_executions': len(execution_history),
        'successful_executions': len(successful),
        'failed_executions': len(failed),
        'overall_success_rate': round(success_rate, 2),
        'patterns_identified': len(patterns),
        'patterns': patterns,
        'insights': patterns[:3] if patterns else []
    }


@tool
def extract_learning_vectors(
    execution_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract learning vectors from execution data for future similarity matching.

    Args:
        execution_data: Complete execution record

    Returns:
        Feature vector and metadata
    """
    vector = {
        'execution_id': execution_data.get('execution_id', 'unknown'),
        'timestamp': execution_data.get('timestamp', 0),

        # Workload features
        'features': {
            'data_volume_gb': execution_data.get('data_volume_gb', 0),
            'file_count': execution_data.get('file_count', 0),
            'complexity_score': execution_data.get('complexity_score', 0),
            'duration_minutes': execution_data.get('duration_minutes', 0),
            'memory_mb': execution_data.get('memory_mb', 0),
            'cpu_utilization': execution_data.get('cpu_utilization', 0)
        },

        # Decision outcomes
        'outcomes': {
            'platform': execution_data.get('platform', 'unknown'),
            'success': execution_data.get('success', False),
            'cost_usd': execution_data.get('cost_usd', 0),
            'quality_score': execution_data.get('quality_score', 0)
        },

        # Contextual metadata
        'metadata': {
            'job_name': execution_data.get('job_name', 'unknown'),
            'source': execution_data.get('source', 'unknown'),
            'destination': execution_data.get('destination', 'unknown')
        }
    }

    return {
        'vector': vector,
        'vector_id': f"{vector['execution_id']}_{vector['timestamp']}",
        'ready_for_storage': True,
        's3_key': f"learning-vectors/{vector['execution_id']}.json"
    }


@tool
def recommend_based_on_history(
    similar_workloads: List[Dict[str, Any]],
    current_requirements: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate recommendations based on historical similar workloads.

    Args:
        similar_workloads: List of similar workload results
        current_requirements: Current job requirements

    Returns:
        Recommendations with confidence scores
    """
    if not similar_workloads:
        return {
            'has_recommendations': False,
            'message': 'No similar workloads found'
        }

    # Aggregate recommendations by platform
    platform_votes = {}
    for workload in similar_workloads:
        platform = workload.get('platform_used', 'unknown')
        if platform not in platform_votes:
            platform_votes[platform] = {
                'count': 0,
                'total_similarity': 0,
                'avg_cost': 0,
                'avg_duration': 0,
                'success_count': 0
            }

        platform_votes[platform]['count'] += 1
        platform_votes[platform]['total_similarity'] += workload.get('similarity_score', 0)
        platform_votes[platform]['avg_cost'] += workload.get('cost_usd', 0)
        platform_votes[platform]['avg_duration'] += workload.get('duration_minutes', 0)
        if workload.get('success', False):
            platform_votes[platform]['success_count'] += 1

    # Calculate scores for each platform
    recommendations = []
    for platform, stats in platform_votes.items():
        count = stats['count']
        avg_similarity = stats['total_similarity'] / count
        avg_cost = stats['avg_cost'] / count
        avg_duration = stats['avg_duration'] / count
        success_rate = stats['success_count'] / count

        confidence = (avg_similarity * 0.5 + success_rate * 0.5)

        recommendations.append({
            'platform': platform,
            'confidence_score': round(confidence, 2),
            'based_on_executions': count,
            'avg_similarity': round(avg_similarity, 2),
            'expected_cost_usd': round(avg_cost, 2),
            'expected_duration_minutes': round(avg_duration, 1),
            'success_rate': round(success_rate, 2),
            'reasoning': f'Based on {count} similar workloads with {success_rate*100:.0f}% success rate'
        })

    recommendations.sort(key=lambda x: x['confidence_score'], reverse=True)

    return {
        'has_recommendations': True,
        'top_recommendation': recommendations[0] if recommendations else None,
        'all_recommendations': recommendations,
        'confidence_level': 'high' if recommendations[0]['confidence_score'] > 0.8 else 'medium'
    }
