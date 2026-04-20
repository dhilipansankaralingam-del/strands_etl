"""
Lambda Function Handler for Decision Agent
Implements platform selection logic called by AWS Bedrock Agent
"""

import json
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import numpy as np
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
s3 = boto3.client('s3')
opensearch_client = None  # Initialize in handler

# Configuration
LEARNING_BUCKET = 'strands-etl-learning'
OPENSEARCH_ENDPOINT = None  # Set from environment


def get_opensearch_client():
    """Get OpenSearch client for Knowledge Base queries"""
    global opensearch_client

    if opensearch_client is None:
        import os
        endpoint = os.environ.get('OPENSEARCH_ENDPOINT')
        region = os.environ.get('AWS_REGION', 'us-east-1')

        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            'aoss',
            session_token=credentials.token
        )

        opensearch_client = OpenSearch(
            hosts=[{'host': endpoint, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    return opensearch_client


def search_learning_vectors(workload: Dict[str, Any], limit: int = 10) -> Dict[str, Any]:
    """
    Search for similar historical workloads using vector similarity.

    This function:
    1. Converts workload characteristics to feature vector
    2. Searches OpenSearch for similar vectors
    3. Returns top N most similar executions
    """
    try:
        logger.info(f"Searching for similar workloads: {workload}")

        # Extract features
        features = extract_workload_features(workload)

        # Try OpenSearch first (Knowledge Base)
        try:
            client = get_opensearch_client()

            # Construct k-NN query
            query = {
                "size": limit,
                "query": {
                    "knn": {
                        "embedding": {
                            "vector": features_to_embedding(features),
                            "k": limit
                        }
                    }
                },
                "_source": [
                    "workload_characteristics",
                    "execution_metrics",
                    "decision_context"
                ]
            }

            response = client.search(
                index="learning-vectors",
                body=query
            )

            similar_workloads = []
            for hit in response['hits']['hits']:
                source = hit['_source']
                similar_workloads.append({
                    'similarity_score': hit['_score'],
                    'platform_used': source.get('execution_metrics', {}).get('platform_used'),
                    'execution_time_seconds': source.get('execution_metrics', {}).get('execution_time_seconds'),
                    'success': source.get('execution_metrics', {}).get('success', False),
                    'quality_score': source.get('execution_metrics', {}).get('data_quality_score'),
                    'workload_characteristics': source.get('workload_characteristics', {})
                })

            return {
                'similar_workloads': similar_workloads,
                'total_found': response['hits']['total']['value'],
                'source': 'opensearch'
            }

        except Exception as e:
            logger.warning(f"OpenSearch query failed, falling back to S3: {e}")

            # Fallback to S3 scan
            return search_s3_learning_vectors(workload, features, limit)

    except Exception as e:
        logger.error(f"Error searching learning vectors: {e}")
        return {
            'similar_workloads': [],
            'total_found': 0,
            'error': str(e)
        }


def search_s3_learning_vectors(workload: Dict[str, Any], features: Dict[str, float], limit: int) -> Dict[str, Any]:
    """Fallback: Search S3 learning vectors using cosine similarity"""
    try:
        # List recent learning vectors from S3
        response = s3.list_objects_v2(
            Bucket=LEARNING_BUCKET,
            Prefix='learning/vectors/learning/',
            MaxKeys=100  # Scan last 100 executions
        )

        similar_workloads = []

        if 'Contents' in response:
            for obj in response['Contents']:
                try:
                    # Load vector
                    vector_data = s3.get_object(Bucket=LEARNING_BUCKET, Key=obj['Key'])
                    vector = json.loads(vector_data['Body'].read().decode('utf-8'))

                    # Calculate similarity
                    vector_features = extract_workload_features(
                        vector.get('workload_characteristics', {})
                    )
                    similarity = calculate_cosine_similarity(features, vector_features)

                    if similarity > 0.5:  # Threshold
                        similar_workloads.append({
                            'similarity_score': similarity,
                            'platform_used': vector.get('execution_metrics', {}).get('platform_used'),
                            'execution_time_seconds': vector.get('execution_metrics', {}).get('execution_time_seconds'),
                            'success': vector.get('execution_metrics', {}).get('success', False),
                            'quality_score': vector.get('execution_metrics', {}).get('data_quality_score'),
                            'workload_characteristics': vector.get('workload_characteristics', {})
                        })

                except Exception as e:
                    logger.warning(f"Error processing vector {obj['Key']}: {e}")
                    continue

        # Sort by similarity and limit
        similar_workloads.sort(key=lambda x: x['similarity_score'], reverse=True)

        return {
            'similar_workloads': similar_workloads[:limit],
            'total_found': len(similar_workloads),
            'source': 's3'
        }

    except Exception as e:
        logger.error(f"S3 search failed: {e}")
        return {
            'similar_workloads': [],
            'total_found': 0,
            'error': str(e)
        }


def extract_workload_features(workload: Dict[str, Any]) -> Dict[str, float]:
    """Convert workload characteristics to numeric features"""
    volume_map = {'low': 1, 'medium': 2, 'high': 3, 'very_high': 4}
    complexity_map = {'low': 1, 'medium': 2, 'high': 3, 'very_high': 4}
    criticality_map = {'low': 1, 'medium': 2, 'high': 3, 'critical': 4}

    return {
        'data_volume': volume_map.get(workload.get('data_volume', 'medium'), 2),
        'complexity': complexity_map.get(workload.get('complexity', 'medium'), 2),
        'criticality': criticality_map.get(workload.get('criticality', 'medium'), 2),
        'estimated_runtime': workload.get('estimated_runtime_minutes', 30) / 60.0
    }


def features_to_embedding(features: Dict[str, float]) -> List[float]:
    """Convert features to embedding vector for k-NN search"""
    # Simple feature vector (in production, use proper embedding model)
    return [
        features.get('data_volume', 2),
        features.get('complexity', 2),
        features.get('criticality', 2),
        features.get('estimated_runtime', 0.5)
    ]


def calculate_cosine_similarity(features1: Dict[str, float], features2: Dict[str, float]) -> float:
    """Calculate cosine similarity between two feature vectors"""
    common_keys = set(features1.keys()) & set(features2.keys())
    if not common_keys:
        return 0.0

    vec1 = np.array([features1[k] for k in common_keys])
    vec2 = np.array([features2[k] for k in common_keys])

    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return float(dot_product / (norm1 * norm2))


def calculate_platform_scores(
    workload: Dict[str, Any],
    similar_workloads: List[Dict[str, Any]],
    constraints: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Calculate scores for each platform based on workload and historical patterns.

    Scoring algorithm:
    1. Rule-based initial scores (based on workload characteristics)
    2. Historical success rate adjustment
    3. Constraint penalty application
    4. Normalization
    """
    try:
        logger.info("Calculating platform scores")

        scores = {
            'glue': {'score': 0.0, 'reasoning': [], 'estimated_cost': 0, 'estimated_time_minutes': 0},
            'emr': {'score': 0.0, 'reasoning': [], 'estimated_cost': 0, 'estimated_time_minutes': 0},
            'lambda': {'score': 0.0, 'reasoning': [], 'estimated_cost': 0, 'estimated_time_minutes': 0}
        }

        # Rule-based scoring
        data_volume = workload.get('data_volume', 'medium')
        complexity = workload.get('complexity', 'medium')

        # Glue scoring
        if data_volume in ['high', 'very_high']:
            scores['glue']['score'] += 0.4
            scores['glue']['reasoning'].append("High data volume favors Glue")
        if complexity in ['high', 'very_high']:
            scores['glue']['score'] += 0.3
            scores['glue']['reasoning'].append("Complex transformations suit Glue")

        # EMR scoring
        if data_volume == 'very_high':
            scores['emr']['score'] += 0.5
            scores['emr']['reasoning'].append("Very high volume ideal for EMR")
        if complexity == 'very_high':
            scores['emr']['score'] += 0.4
            scores['emr']['reasoning'].append("Very complex workload suits EMR")

        # Lambda scoring
        if data_volume == 'low':
            scores['lambda']['score'] += 0.5
            scores['lambda']['reasoning'].append("Low volume perfect for Lambda")
        if complexity == 'low':
            scores['lambda']['score'] += 0.3
            scores['lambda']['reasoning'].append("Simple transformation suits Lambda")

        # Historical pattern adjustment
        if similar_workloads:
            platform_stats = {'glue': [], 'emr': [], 'lambda': []}

            for similar in similar_workloads:
                platform = similar.get('platform_used')
                if platform in platform_stats and similar.get('success'):
                    platform_stats[platform].append({
                        'similarity': similar.get('similarity_score', 0),
                        'time': similar.get('execution_time_seconds', 0)
                    })

            # Adjust scores based on historical success
            for platform, stats in platform_stats.items():
                if stats:
                    success_weight = len(stats) / len(similar_workloads)
                    avg_similarity = np.mean([s['similarity'] for s in stats])
                    scores[platform]['score'] += success_weight * avg_similarity * 0.3
                    scores[platform]['reasoning'].append(
                        f"Historical success: {len(stats)}/{len(similar_workloads)} similar workloads"
                    )

                    # Estimate time
                    if stats:
                        scores[platform]['estimated_time_minutes'] = np.mean([s['time'] for s in stats]) / 60

        # Apply constraints
        if constraints:
            max_cost = constraints.get('max_cost')
            max_time = constraints.get('max_time_minutes')

            # Cost/time penalties would go here
            # For now, simplified

        # Normalize scores
        max_score = max(s['score'] for s in scores.values()) or 1.0
        for platform in scores:
            scores[platform]['score'] = min(scores[platform]['score'] / max_score, 1.0)
            scores[platform]['reasoning'] = ' | '.join(scores[platform]['reasoning']) or 'No specific factors'

        # Determine recommendation
        recommended_platform = max(scores.items(), key=lambda x: x[1]['score'])[0]
        confidence = scores[recommended_platform]['score']

        return {
            'platform_scores': scores,
            'recommended_platform': recommended_platform,
            'confidence': confidence
        }

    except Exception as e:
        logger.error(f"Error calculating scores: {e}")
        return {
            'platform_scores': scores,
            'recommended_platform': 'glue',
            'confidence': 0.5,
            'error': str(e)
        }


def get_platform_statistics(platform: str = 'all', days: int = 30) -> Dict[str, Any]:
    """Get aggregate statistics for platforms"""
    try:
        # This would query from OpenSearch or aggregate S3 vectors
        # Simplified for now
        return {
            'glue': {
                'total_executions': 150,
                'success_rate': 0.92,
                'avg_execution_time_seconds': 1800,
                'avg_cost': 5.50
            },
            'emr': {
                'total_executions': 45,
                'success_rate': 0.88,
                'avg_execution_time_seconds': 3600,
                'avg_cost': 12.00
            },
            'lambda': {
                'total_executions': 320,
                'success_rate': 0.95,
                'avg_execution_time_seconds': 180,
                'avg_cost': 0.50
            }
        }
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return {}


def lambda_handler(event, context):
    """
    Main Lambda handler for Bedrock Agent action group

    Event structure from Bedrock Agent:
    {
        "actionGroup": "platform-selection-tools",
        "apiPath": "/search_learning_vectors",
        "httpMethod": "POST",
        "requestBody": {
            "content": {
                "application/json": {
                    "properties": [...]
                }
            }
        },
        "agent": {...},
        "sessionId": "...",
        "sessionAttributes": {...}
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract action details
        api_path = event.get('apiPath', '')
        http_method = event.get('httpMethod', 'POST')

        # Parse request body
        request_body = {}
        if 'requestBody' in event:
            content = event['requestBody'].get('content', {})
            json_content = content.get('application/json', {})
            properties = json_content.get('properties', [])

            # Convert properties array to dict
            for prop in properties:
                request_body[prop['name']] = prop['value']

        # Route to appropriate function
        if api_path == '/search_learning_vectors':
            workload = json.loads(request_body.get('workload', '{}'))
            limit = int(request_body.get('limit', 10))
            result = search_learning_vectors(workload, limit)

        elif api_path == '/calculate_platform_scores':
            workload = json.loads(request_body.get('workload', '{}'))
            similar_workloads = json.loads(request_body.get('similar_workloads', '[]'))
            constraints = json.loads(request_body.get('constraints', '{}'))
            result = calculate_platform_scores(workload, similar_workloads, constraints)

        elif api_path == '/get_platform_statistics':
            platform = request_body.get('platform', 'all')
            days = int(request_body.get('days', 30))
            result = get_platform_statistics(platform, days)

        else:
            result = {'error': f'Unknown API path: {api_path}'}

        # Return response in Bedrock Agent format
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': event.get('actionGroup'),
                'apiPath': api_path,
                'httpMethod': http_method,
                'httpStatusCode': 200,
                'responseBody': {
                    'application/json': {
                        'body': json.dumps(result)
                    }
                }
            }
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}", exc_info=True)

        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': event.get('actionGroup'),
                'apiPath': event.get('apiPath'),
                'httpMethod': event.get('httpMethod'),
                'httpStatusCode': 500,
                'responseBody': {
                    'application/json': {
                        'body': json.dumps({'error': str(e)})
                    }
                }
            }
        }
