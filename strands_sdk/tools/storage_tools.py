"""
Storage Tools for Strands SDK Agents
=====================================

Tools for persisting data:
- Audit logs (pipe-delimited files)
- Recommendations storage
- Execution history for ML training
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

from strands import tool

logger = logging.getLogger("strands.tools.storage")

# Default storage paths
DATA_DIR = Path("data")
AUDIT_DIR = DATA_DIR / "audit_logs"
RECOMMENDATIONS_DIR = DATA_DIR / "recommendations"
HISTORY_DIR = DATA_DIR / "execution_history"
MODELS_DIR = DATA_DIR / "models"

# Ensure directories exist
for directory in [AUDIT_DIR, RECOMMENDATIONS_DIR, HISTORY_DIR, MODELS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


@tool
def write_audit_log(
    job_name: str,
    event_type: str,
    event_data: Dict[str, Any],
    agent_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Write an audit log entry as a pipe-delimited file.

    Args:
        job_name: Name of the ETL job
        event_type: Type of event (e.g., 'job_started', 'sizing_complete')
        event_data: Event data to log
        agent_name: Name of the agent writing the log

    Returns:
        Dictionary with file path and status
    """
    timestamp = datetime.utcnow()
    date_str = timestamp.strftime('%Y%m%d')
    time_str = timestamp.strftime('%H%M%S')

    # Create filename
    filename = f"{job_name}_{event_type}_{date_str}_{time_str}.psv"
    filepath = AUDIT_DIR / filename

    # Prepare record
    record = {
        "timestamp": timestamp.isoformat(),
        "job_name": job_name,
        "event_type": event_type,
        "agent_name": agent_name or "unknown",
        **event_data
    }

    # Write as pipe-delimited
    try:
        headers = list(record.keys())
        values = [str(record.get(h, '')) for h in headers]

        with open(filepath, 'w') as f:
            f.write('|'.join(headers) + '\n')
            f.write('|'.join(values) + '\n')

        logger.info(f"Wrote audit log: {filepath}")
        return {
            "status": "success",
            "file_path": str(filepath),
            "record_count": 1
        }

    except Exception as e:
        logger.error(f"Failed to write audit log: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


@tool
def store_recommendations(
    job_name: str,
    recommendations: List[Dict[str, Any]],
    execution_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Store recommendations from agent analysis.

    Args:
        job_name: Name of the ETL job
        recommendations: List of recommendation dictionaries
        execution_id: Unique execution identifier

    Returns:
        Dictionary with storage status
    """
    timestamp = datetime.utcnow()
    date_str = timestamp.strftime('%Y%m%d_%H%M%S')
    exec_id = execution_id or f"exec_{date_str}"

    filename = f"{job_name}_recommendations_{date_str}.json"
    filepath = RECOMMENDATIONS_DIR / filename

    try:
        data = {
            "job_name": job_name,
            "execution_id": exec_id,
            "timestamp": timestamp.isoformat(),
            "recommendations": recommendations,
            "total_count": len(recommendations)
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Stored {len(recommendations)} recommendations: {filepath}")
        return {
            "status": "success",
            "file_path": str(filepath),
            "recommendation_count": len(recommendations)
        }

    except Exception as e:
        logger.error(f"Failed to store recommendations: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


@tool
def load_execution_history(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Load execution history for ML model training.

    Args:
        limit: Maximum number of records to return

    Returns:
        List of execution history records
    """
    history_file = HISTORY_DIR / "execution_history.json"

    if not history_file.exists():
        return []

    try:
        with open(history_file, 'r') as f:
            history = json.load(f)

        # Return most recent records
        return history[-limit:]

    except Exception as e:
        logger.error(f"Failed to load execution history: {e}")
        return []


@tool
def save_execution_history(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save an execution record to history for ML training.

    Args:
        record: Execution record with metrics

    Returns:
        Dictionary with save status
    """
    history_file = HISTORY_DIR / "execution_history.json"

    try:
        # Load existing history
        history = []
        if history_file.exists():
            with open(history_file, 'r') as f:
                history = json.load(f)

        # Add new record with timestamp
        record['saved_at'] = datetime.utcnow().isoformat()
        history.append(record)

        # Keep last 1000 records
        history = history[-1000:]

        # Save
        with open(history_file, 'w') as f:
            json.dump(history, f, indent=2)

        logger.info(f"Saved execution record. Total history: {len(history)}")
        return {
            "status": "success",
            "total_records": len(history)
        }

    except Exception as e:
        logger.error(f"Failed to save execution history: {e}")
        return {
            "status": "error",
            "error": str(e)
        }
