"""
Quality Agent Tools - Data quality validation and profiling
"""
from typing import Dict, Any, List
from strands import tool
import re

@tool
def analyze_data_completeness(
    total_records: int,
    null_counts: Dict[str, int],
    required_fields: List[str]
) -> Dict[str, Any]:
    """
    Analyze data completeness by checking for null values in critical fields.

    Args:
        total_records: Total number of records in dataset
        null_counts: Dictionary mapping field names to null counts
        required_fields: List of fields that should not have nulls

    Returns:
        Completeness analysis with scores
    """
    if total_records == 0:
        return {'error': 'No records to analyze'}

    field_completeness = {}
    issues = []

    for field in null_counts:
        nulls = null_counts[field]
        completeness = ((total_records - nulls) / total_records) * 100

        field_completeness[field] = {
            'null_count': nulls,
            'non_null_count': total_records - nulls,
            'completeness_percentage': round(completeness, 2)
        }

        # Check required fields
        if field in required_fields and nulls > 0:
            issues.append({
                'field': field,
                'severity': 'high',
                'message': f'Required field "{field}" has {nulls} null values ({100-completeness:.2f}% incomplete)'
            })
        elif completeness < 95:
            issues.append({
                'field': field,
                'severity': 'medium',
                'message': f'Field "{field}" has low completeness: {completeness:.2f}%'
            })

    # Calculate overall completeness score
    avg_completeness = sum(f['completeness_percentage'] for f in field_completeness.values()) / len(field_completeness)

    return {
        'total_records': total_records,
        'fields_analyzed': len(field_completeness),
        'field_completeness': field_completeness,
        'overall_completeness_score': round(avg_completeness / 100, 3),
        'issues': issues,
        'passed': len(issues) == 0
    }


@tool
def check_data_accuracy(
    sample_data: Dict[str, List[Any]],
    validation_rules: Dict[str, str]
) -> Dict[str, Any]:
    """
    Check data accuracy using validation rules.

    Args:
        sample_data: Dictionary of field names to sample values
        validation_rules: Dictionary of field names to regex patterns or range rules

    Returns:
        Accuracy analysis with violations
    """
    violations = []
    field_accuracy = {}

    for field, values in sample_data.items():
        if field not in validation_rules:
            continue

        rule = validation_rules[field]
        valid_count = 0
        invalid_values = []

        for value in values:
            if value is None:
                continue

            # Check regex pattern
            if rule.startswith('regex:'):
                pattern = rule.replace('regex:', '')
                if re.match(pattern, str(value)):
                    valid_count += 1
                else:
                    invalid_values.append(value)

            # Check numeric range
            elif rule.startswith('range:'):
                try:
                    min_val, max_val = map(float, rule.replace('range:', '').split(','))
                    if min_val <= float(value) <= max_val:
                        valid_count += 1
                    else:
                        invalid_values.append(value)
                except (ValueError, TypeError):
                    invalid_values.append(value)

        total_checked = len([v for v in values if v is not None])
        if total_checked > 0:
            accuracy = (valid_count / total_checked) * 100
            field_accuracy[field] = {
                'valid_count': valid_count,
                'invalid_count': len(invalid_values),
                'accuracy_percentage': round(accuracy, 2),
                'sample_invalid_values': invalid_values[:5]
            }

            if accuracy < 95:
                violations.append({
                    'field': field,
                    'severity': 'high' if accuracy < 90 else 'medium',
                    'message': f'Field "{field}" has accuracy of {accuracy:.2f}%',
                    'invalid_count': len(invalid_values)
                })

    avg_accuracy = sum(f['accuracy_percentage'] for f in field_accuracy.values()) / len(field_accuracy) if field_accuracy else 100

    return {
        'fields_validated': len(field_accuracy),
        'field_accuracy': field_accuracy,
        'overall_accuracy_score': round(avg_accuracy / 100, 3),
        'violations': violations,
        'passed': len(violations) == 0
    }


@tool
def detect_data_duplicates(
    total_records: int,
    duplicate_count: int,
    key_fields: List[str]
) -> Dict[str, Any]:
    """
    Detect and analyze duplicate records.

    Args:
        total_records: Total number of records
        duplicate_count: Number of duplicate records found
        key_fields: Fields used to identify duplicates

    Returns:
        Duplicate analysis
    """
    if total_records == 0:
        return {'error': 'No records to analyze'}

    duplicate_percentage = (duplicate_count / total_records) * 100
    unique_records = total_records - duplicate_count

    severity = 'high' if duplicate_percentage > 5 else 'medium' if duplicate_percentage > 1 else 'low'

    return {
        'total_records': total_records,
        'duplicate_count': duplicate_count,
        'unique_records': unique_records,
        'duplicate_percentage': round(duplicate_percentage, 2),
        'key_fields_used': key_fields,
        'severity': severity,
        'recommendation': 'Add deduplication step before loading' if duplicate_percentage > 1 else 'Duplication within acceptable range',
        'passed': duplicate_percentage < 1
    }


@tool
def validate_schema_consistency(
    expected_schema: Dict[str, str],
    actual_schema: Dict[str, str]
) -> Dict[str, Any]:
    """
    Validate that actual schema matches expected schema.

    Args:
        expected_schema: Dictionary of field names to expected data types
        actual_schema: Dictionary of field names to actual data types

    Returns:
        Schema validation results
    """
    mismatches = []
    missing_fields = []
    extra_fields = []

    # Check for missing fields
    for field in expected_schema:
        if field not in actual_schema:
            missing_fields.append({
                'field': field,
                'expected_type': expected_schema[field],
                'severity': 'high'
            })

    # Check for type mismatches and extra fields
    for field in actual_schema:
        if field not in expected_schema:
            extra_fields.append({
                'field': field,
                'actual_type': actual_schema[field],
                'severity': 'low'
            })
        elif expected_schema[field] != actual_schema[field]:
            mismatches.append({
                'field': field,
                'expected_type': expected_schema[field],
                'actual_type': actual_schema[field],
                'severity': 'high'
            })

    all_issues = mismatches + missing_fields + extra_fields

    return {
        'schema_matches': len(all_issues) == 0,
        'fields_validated': len(expected_schema),
        'mismatches': mismatches,
        'missing_fields': missing_fields,
        'extra_fields': extra_fields,
        'total_issues': len(all_issues),
        'passed': len(mismatches) == 0 and len(missing_fields) == 0
    }


@tool
def calculate_quality_score(
    completeness_score: float,
    accuracy_score: float,
    consistency_score: float,
    timeliness_score: float,
    validity_score: float
) -> Dict[str, Any]:
    """
    Calculate overall data quality score from individual dimensions.

    Args:
        completeness_score: Score for completeness (0-1)
        accuracy_score: Score for accuracy (0-1)
        consistency_score: Score for consistency (0-1)
        timeliness_score: Score for timeliness (0-1)
        validity_score: Score for validity (0-1)

    Returns:
        Overall quality score and grade
    """
    # Weighted average (completeness and accuracy are most important)
    weights = {
        'completeness': 0.25,
        'accuracy': 0.25,
        'consistency': 0.20,
        'timeliness': 0.15,
        'validity': 0.15
    }

    overall_score = (
        completeness_score * weights['completeness'] +
        accuracy_score * weights['accuracy'] +
        consistency_score * weights['consistency'] +
        timeliness_score * weights['timeliness'] +
        validity_score * weights['validity']
    )

    # Determine grade
    if overall_score >= 0.95:
        grade = 'A'
        status = 'excellent'
    elif overall_score >= 0.90:
        grade = 'B'
        status = 'good'
    elif overall_score >= 0.80:
        grade = 'C'
        status = 'acceptable'
    elif overall_score >= 0.70:
        grade = 'D'
        status = 'needs improvement'
    else:
        grade = 'F'
        status = 'poor'

    return {
        'overall_quality_score': round(overall_score, 3),
        'grade': grade,
        'status': status,
        'dimension_scores': {
            'completeness': round(completeness_score, 3),
            'accuracy': round(accuracy_score, 3),
            'consistency': round(consistency_score, 3),
            'timeliness': round(timeliness_score, 3),
            'validity': round(validity_score, 3)
        },
        'weights_used': weights,
        'passed': overall_score >= 0.80
    }


@tool
def analyze_pyspark_script(
    script_content: str
) -> Dict[str, Any]:
    """
    Analyze PySpark script for anti-patterns and performance issues.

    Args:
        script_content: The PySpark script code as a string

    Returns:
        Analysis with detected issues and recommendations
    """
    issues = []

    # Check for multiple count() operations
    count_operations = script_content.count('.count()')
    if count_operations > 2:
        issues.append({
            'type': 'multiple_counts',
            'severity': 'high',
            'count': count_operations,
            'message': f'Found {count_operations} .count() operations. Each causes a full scan.',
            'recommendation': 'Cache DataFrame and reuse count result'
        })

    # Check for SELECT *
    if 'SELECT *' in script_content.upper() or 'select(\"*\")' in script_content:
        issues.append({
            'type': 'select_star',
            'severity': 'medium',
            'message': 'Using SELECT * reads all columns unnecessarily',
            'recommendation': 'Select only required columns'
        })

    # Check for collect()
    if '.collect()' in script_content:
        issues.append({
            'type': 'collect_usage',
            'severity': 'high',
            'message': 'Using .collect() loads all data to driver memory',
            'recommendation': 'Use .take(n) or avoid collecting large datasets'
        })

    # Check for broadcast hint
    if '.join(' in script_content and 'broadcast(' not in script_content:
        issues.append({
            'type': 'missing_broadcast',
            'severity': 'medium',
            'message': 'Joins detected without broadcast hints',
            'recommendation': 'Use broadcast() for small dimension tables (<100MB)'
        })

    # Check for coalesce/repartition before write
    if '.write.' in script_content and '.coalesce(' not in script_content and '.repartition(' not in script_content:
        issues.append({
            'type': 'missing_coalesce',
            'severity': 'low',
            'message': 'Writing without coalesce/repartition may create many small files',
            'recommendation': 'Add .coalesce(N) before writing to control output file count'
        })

    return {
        'script_analyzed': True,
        'total_issues': len(issues),
        'issues_by_severity': {
            'high': len([i for i in issues if i['severity'] == 'high']),
            'medium': len([i for i in issues if i['severity'] == 'medium']),
            'low': len([i for i in issues if i['severity'] == 'low'])
        },
        'issues': issues,
        'passed': len([i for i in issues if i['severity'] == 'high']) == 0
    }
