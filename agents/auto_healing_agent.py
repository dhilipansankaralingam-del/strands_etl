"""
Auto-Healing Agent
==================
Handles platform fallback and automatic error recovery.

Features:
- Platform fallback when primary platform fails
- Automatic error detection and classification
- Self-healing strategies for common errors
- Code modification for recoverable errors
- Resource optimization on retry
"""

import json
import re
import logging
import boto3
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ErrorType(Enum):
    """Classification of error types for healing strategies."""
    OUT_OF_MEMORY = "out_of_memory"
    TIMEOUT = "timeout"
    DATA_SKEW = "data_skew"
    SHUFFLE_FAILURE = "shuffle_failure"
    EXECUTOR_LOST = "executor_lost"
    DISK_SPACE = "disk_space"
    CONNECTION_ERROR = "connection_error"
    SCHEMA_MISMATCH = "schema_mismatch"
    NULL_CONSTRAINT = "null_constraint"
    PARTITION_ERROR = "partition_error"
    PERMISSION_ERROR = "permission_error"
    RESOURCE_LIMIT = "resource_limit"
    UNKNOWN = "unknown"


class HealingStrategy(Enum):
    """Available healing strategies."""
    INCREASE_MEMORY = "increase_memory"
    ADD_WORKERS = "add_workers"
    REDUCE_PARTITIONS = "reduce_partitions"
    ENABLE_DISK_SPILL = "enable_disk_spill"
    EXTEND_TIMEOUT = "extend_timeout"
    OPTIMIZE_QUERY = "optimize_query"
    SALT_KEYS = "salt_keys"
    BROADCAST_JOIN = "broadcast_join"
    REPARTITION = "repartition"
    RETRY = "retry"
    REDUCE_PARALLELISM = "reduce_parallelism"
    CHECKPOINT = "checkpoint"
    PLATFORM_FALLBACK = "platform_fallback"


class AutoHealingAgent:
    """
    Agent that handles automatic error recovery and platform fallback.
    """

    # Error patterns for classification
    ERROR_PATTERNS = {
        ErrorType.OUT_OF_MEMORY: [
            r"OutOfMemoryError",
            r"Java heap space",
            r"Container killed by YARN for exceeding memory limits",
            r"ExecutorLostFailure.*memory",
            r"GC overhead limit exceeded",
            r"Unable to acquire.*memory"
        ],
        ErrorType.TIMEOUT: [
            r"timeout",
            r"Job.*exceeded.*time limit",
            r"Task.*timed out",
            r"Connection timed out",
            r"Read timed out"
        ],
        ErrorType.DATA_SKEW: [
            r"data skew",
            r"Skewed partition",
            r"Task.*significantly longer",
            r"Stage.*uneven task distribution"
        ],
        ErrorType.SHUFFLE_FAILURE: [
            r"Shuffle.*failed",
            r"FetchFailedException",
            r"Failed to connect to.*shuffle",
            r"Shuffle block.*not found"
        ],
        ErrorType.EXECUTOR_LOST: [
            r"ExecutorLostFailure",
            r"Executor.*lost",
            r"Container.*exited with",
            r"Executor heartbeat timed out"
        ],
        ErrorType.DISK_SPACE: [
            r"No space left on device",
            r"Disk space",
            r"IOException.*disk"
        ],
        ErrorType.CONNECTION_ERROR: [
            r"Connection refused",
            r"Unable to connect",
            r"Network is unreachable",
            r"Connection reset"
        ],
        ErrorType.SCHEMA_MISMATCH: [
            r"Schema mismatch",
            r"Column.*not found",
            r"AnalysisException.*cannot resolve",
            r"Field.*does not exist"
        ],
        ErrorType.NULL_CONSTRAINT: [
            r"NULL constraint",
            r"NullPointerException",
            r"Column.*cannot be null"
        ],
        ErrorType.PARTITION_ERROR: [
            r"Partition.*not found",
            r"Too many partitions",
            r"Partition column.*mismatch"
        ],
        ErrorType.PERMISSION_ERROR: [
            r"Access Denied",
            r"Permission denied",
            r"Unauthorized",
            r"Forbidden"
        ],
        ErrorType.RESOURCE_LIMIT: [
            r"Resource limit",
            r"Quota exceeded",
            r"Rate limit"
        ]
    }

    # Default healing strategies for each error type
    DEFAULT_HEALING_STRATEGIES = {
        ErrorType.OUT_OF_MEMORY: [
            HealingStrategy.INCREASE_MEMORY,
            HealingStrategy.ADD_WORKERS,
            HealingStrategy.ENABLE_DISK_SPILL,
            HealingStrategy.REDUCE_PARTITIONS
        ],
        ErrorType.TIMEOUT: [
            HealingStrategy.EXTEND_TIMEOUT,
            HealingStrategy.ADD_WORKERS,
            HealingStrategy.OPTIMIZE_QUERY
        ],
        ErrorType.DATA_SKEW: [
            HealingStrategy.SALT_KEYS,
            HealingStrategy.BROADCAST_JOIN,
            HealingStrategy.REPARTITION
        ],
        ErrorType.SHUFFLE_FAILURE: [
            HealingStrategy.RETRY,
            HealingStrategy.REDUCE_PARALLELISM,
            HealingStrategy.CHECKPOINT
        ],
        ErrorType.EXECUTOR_LOST: [
            HealingStrategy.RETRY,
            HealingStrategy.REDUCE_PARALLELISM,
            HealingStrategy.ADD_WORKERS
        ],
        ErrorType.DISK_SPACE: [
            HealingStrategy.PLATFORM_FALLBACK
        ],
        ErrorType.CONNECTION_ERROR: [
            HealingStrategy.RETRY,
            HealingStrategy.PLATFORM_FALLBACK
        ],
        ErrorType.SCHEMA_MISMATCH: [
            HealingStrategy.PLATFORM_FALLBACK  # Needs code fix
        ],
        ErrorType.NULL_CONSTRAINT: [
            HealingStrategy.PLATFORM_FALLBACK  # Needs code fix
        ],
        ErrorType.PARTITION_ERROR: [
            HealingStrategy.REPARTITION,
            HealingStrategy.RETRY
        ],
        ErrorType.PERMISSION_ERROR: [
            HealingStrategy.PLATFORM_FALLBACK  # Needs IAM fix
        ],
        ErrorType.RESOURCE_LIMIT: [
            HealingStrategy.RETRY,
            HealingStrategy.PLATFORM_FALLBACK
        ]
    }

    def __init__(self, config: Dict[str, Any], region: str = None):
        """
        Initialize Auto-Healing Agent.

        Args:
            config: Pipeline configuration with auto_healing settings
            region: AWS region
        """
        self.config = config
        self.region = region
        self.healing_config = config.get('execution', {}).get('auto_healing', {})
        self.fallback_config = config.get('execution', {}).get('platform_fallback', {})

        client_kwargs = {'region_name': region} if region else {}
        self.glue = boto3.client('glue', **client_kwargs)
        self.bedrock = boto3.client('bedrock-runtime', **client_kwargs)

        self.heal_attempts = 0
        self.max_heal_attempts = self.healing_config.get('max_heal_attempts', 3)
        self.healing_history = []

    def classify_error(self, error_message: str) -> Tuple[ErrorType, float]:
        """
        Classify the error type from the error message.

        Args:
            error_message: The error message to classify

        Returns:
            Tuple of (ErrorType, confidence_score)
        """
        if not error_message:
            return ErrorType.UNKNOWN, 0.0

        error_message_lower = error_message.lower()

        for error_type, patterns in self.ERROR_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, error_message, re.IGNORECASE):
                    # Calculate confidence based on pattern specificity
                    confidence = 0.9 if len(pattern) > 20 else 0.7
                    logger.info(f"Classified error as {error_type.value} with confidence {confidence}")
                    return error_type, confidence

        return ErrorType.UNKNOWN, 0.0

    def can_heal(self, error_type: ErrorType) -> bool:
        """Check if the error type is healable."""
        if not self.healing_config.get('enabled', False):
            return False

        if self.heal_attempts >= self.max_heal_attempts:
            logger.warning(f"Max heal attempts ({self.max_heal_attempts}) reached")
            return False

        healable_errors = self.healing_config.get('healable_errors', [])
        return error_type.value in healable_errors or error_type == ErrorType.UNKNOWN

    def get_healing_strategy(self, error_type: ErrorType) -> Optional[HealingStrategy]:
        """
        Get the appropriate healing strategy for an error type.

        Args:
            error_type: The classified error type

        Returns:
            The recommended healing strategy
        """
        custom_strategies = self.healing_config.get('healing_strategies', {})

        # Check for custom strategy first
        if error_type.value in custom_strategies:
            strategy_name = custom_strategies[error_type.value]
            try:
                return HealingStrategy(strategy_name)
            except ValueError:
                pass

        # Fall back to default strategies
        default_strategies = self.DEFAULT_HEALING_STRATEGIES.get(error_type, [])
        if default_strategies:
            # Return the first strategy that hasn't been tried
            for strategy in default_strategies:
                if not self._strategy_already_tried(strategy):
                    return strategy

        return HealingStrategy.PLATFORM_FALLBACK

    def _strategy_already_tried(self, strategy: HealingStrategy) -> bool:
        """Check if a strategy has already been tried."""
        return any(h['strategy'] == strategy.value for h in self.healing_history)

    def apply_healing(
        self,
        error_type: ErrorType,
        strategy: HealingStrategy,
        current_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply a healing strategy and return modified configuration.

        Args:
            error_type: The error type being healed
            strategy: The healing strategy to apply
            current_config: Current job configuration

        Returns:
            Modified configuration with healing applied
        """
        self.heal_attempts += 1
        healed_config = current_config.copy()
        healing_actions = []

        execution_config = healed_config.get('execution', {})
        resource_config = execution_config.get('resource_config', {})

        if strategy == HealingStrategy.INCREASE_MEMORY:
            # Upgrade worker type
            current_worker = resource_config.get('worker_type', 'G.1X')
            worker_upgrades = {'G.1X': 'G.2X', 'G.2X': 'G.4X', 'G.4X': 'G.8X'}
            new_worker = worker_upgrades.get(current_worker, 'G.2X')
            resource_config['worker_type'] = new_worker
            healing_actions.append(f"Upgraded worker type from {current_worker} to {new_worker}")

        elif strategy == HealingStrategy.ADD_WORKERS:
            current_workers = resource_config.get('number_of_workers', 5)
            new_workers = min(current_workers * 2, 100)  # Cap at 100
            resource_config['number_of_workers'] = new_workers
            healing_actions.append(f"Increased workers from {current_workers} to {new_workers}")

        elif strategy == HealingStrategy.REDUCE_PARTITIONS:
            # Add Spark config for fewer partitions
            spark_conf = resource_config.get('spark_conf', {})
            spark_conf['spark.sql.shuffle.partitions'] = '100'
            resource_config['spark_conf'] = spark_conf
            healing_actions.append("Reduced shuffle partitions to 100")

        elif strategy == HealingStrategy.ENABLE_DISK_SPILL:
            spark_conf = resource_config.get('spark_conf', {})
            spark_conf['spark.memory.fraction'] = '0.5'
            spark_conf['spark.memory.storageFraction'] = '0.3'
            resource_config['spark_conf'] = spark_conf
            healing_actions.append("Enabled disk spill by reducing memory fraction")

        elif strategy == HealingStrategy.EXTEND_TIMEOUT:
            current_timeout = resource_config.get('timeout_minutes', 60)
            new_timeout = min(current_timeout * 2, 480)  # Cap at 8 hours
            resource_config['timeout_minutes'] = new_timeout
            healing_actions.append(f"Extended timeout from {current_timeout} to {new_timeout} minutes")

        elif strategy == HealingStrategy.BROADCAST_JOIN:
            spark_conf = resource_config.get('spark_conf', {})
            spark_conf['spark.sql.autoBroadcastJoinThreshold'] = '104857600'  # 100MB
            resource_config['spark_conf'] = spark_conf
            healing_actions.append("Increased broadcast join threshold to 100MB")

        elif strategy == HealingStrategy.REPARTITION:
            spark_conf = resource_config.get('spark_conf', {})
            spark_conf['spark.sql.shuffle.partitions'] = '400'
            spark_conf['spark.default.parallelism'] = '400'
            resource_config['spark_conf'] = spark_conf
            healing_actions.append("Increased partitioning for better distribution")

        elif strategy == HealingStrategy.REDUCE_PARALLELISM:
            spark_conf = resource_config.get('spark_conf', {})
            spark_conf['spark.sql.shuffle.partitions'] = '50'
            spark_conf['spark.default.parallelism'] = '50'
            resource_config['spark_conf'] = spark_conf
            healing_actions.append("Reduced parallelism to stabilize executors")

        elif strategy == HealingStrategy.CHECKPOINT:
            spark_conf = resource_config.get('spark_conf', {})
            spark_conf['spark.checkpoint.compress'] = 'true'
            resource_config['spark_conf'] = spark_conf
            healing_actions.append("Enabled checkpoint compression")

        elif strategy == HealingStrategy.RETRY:
            healing_actions.append("Simple retry with same configuration")

        execution_config['resource_config'] = resource_config
        healed_config['execution'] = execution_config

        # Record healing attempt
        self.healing_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'error_type': error_type.value,
            'strategy': strategy.value,
            'actions': healing_actions,
            'attempt': self.heal_attempts
        })

        logger.info(f"Applied healing strategy {strategy.value}: {', '.join(healing_actions)}")

        return healed_config

    def get_fallback_platform(self, current_platform: str) -> Optional[str]:
        """
        Get the next fallback platform.

        Args:
            current_platform: The current platform that failed

        Returns:
            The next platform to try, or None if no fallbacks available
        """
        if not self.fallback_config.get('enabled', True):
            return None

        fallback_order = self.fallback_config.get(
            'fallback_order',
            ['glue', 'emr', 'lambda']
        )

        tried_platforms = [h.get('platform') for h in self.healing_history if h.get('platform')]
        tried_platforms.append(current_platform)

        for platform in fallback_order:
            if platform not in tried_platforms:
                logger.info(f"Fallback: switching from {current_platform} to {platform}")
                return platform

        logger.warning("No more fallback platforms available")
        return None

    def generate_code_fix(self, error_type: ErrorType, error_message: str, code_snippet: str) -> Optional[Dict[str, Any]]:
        """
        Generate code fix recommendations using AI.

        Args:
            error_type: The classified error type
            error_message: Full error message
            code_snippet: The relevant code that caused the error

        Returns:
            Code fix recommendation with before/after
        """
        if not self.healing_config.get('code_modification_allowed', False):
            return None

        prompt = f"""
        You are a PySpark expert. Analyze this error and provide a code fix.

        ERROR TYPE: {error_type.value}
        ERROR MESSAGE: {error_message}

        CODE SNIPPET:
        ```python
        {code_snippet}
        ```

        Provide a JSON response with:
        {{
            "error_analysis": "Brief explanation of what caused the error",
            "fix_description": "Description of the fix",
            "original_code": "The problematic code section",
            "fixed_code": "The corrected code",
            "additional_recommendations": ["list", "of", "recommendations"],
            "confidence": 0.0-1.0
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
            logger.error(f"Failed to generate code fix: {e}")

        return None

    def heal(
        self,
        error_message: str,
        current_config: Dict[str, Any],
        current_platform: str,
        code_snippet: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main healing method that orchestrates the healing process.

        Args:
            error_message: The error message from the failed job
            current_config: Current job configuration
            current_platform: Current execution platform
            code_snippet: Optional code snippet for code-level fixes

        Returns:
            Healing result with new configuration or fallback recommendation
        """
        result = {
            'healed': False,
            'strategy_applied': None,
            'new_config': current_config,
            'fallback_platform': None,
            'code_fix': None,
            'healing_history': self.healing_history,
            'message': ''
        }

        # Classify the error
        error_type, confidence = self.classify_error(error_message)
        result['error_type'] = error_type.value
        result['error_confidence'] = confidence

        # Check if we can heal
        if not self.can_heal(error_type):
            result['message'] = f"Cannot heal error type {error_type.value} or max attempts reached"
            result['fallback_platform'] = self.get_fallback_platform(current_platform)
            return result

        # Get healing strategy
        strategy = self.get_healing_strategy(error_type)
        result['strategy_applied'] = strategy.value

        # If strategy is platform fallback, get next platform
        if strategy == HealingStrategy.PLATFORM_FALLBACK:
            result['fallback_platform'] = self.get_fallback_platform(current_platform)
            result['message'] = f"Recommending platform fallback to {result['fallback_platform']}"

            # Record fallback in history
            self.healing_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'error_type': error_type.value,
                'strategy': strategy.value,
                'platform': current_platform,
                'fallback_to': result['fallback_platform'],
                'attempt': self.heal_attempts + 1
            })

            return result

        # Apply healing strategy
        result['new_config'] = self.apply_healing(error_type, strategy, current_config)
        result['healed'] = True
        result['message'] = f"Applied healing strategy: {strategy.value}"

        # Generate code fix if applicable and code is provided
        if code_snippet and error_type in [
            ErrorType.SCHEMA_MISMATCH,
            ErrorType.NULL_CONSTRAINT,
            ErrorType.DATA_SKEW
        ]:
            code_fix = self.generate_code_fix(error_type, error_message, code_snippet)
            if code_fix:
                result['code_fix'] = code_fix

        return result

    def get_healing_report(self) -> Dict[str, Any]:
        """Generate a report of all healing attempts."""
        return {
            'total_attempts': self.heal_attempts,
            'max_attempts': self.max_heal_attempts,
            'history': self.healing_history,
            'strategies_used': list(set(h['strategy'] for h in self.healing_history)),
            'error_types_encountered': list(set(h['error_type'] for h in self.healing_history))
        }
