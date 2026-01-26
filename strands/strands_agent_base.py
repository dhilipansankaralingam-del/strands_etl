"""
Strands Agent Base - Foundation for all independent Strands agents.
Provides autonomous execution, message handling, and lifecycle management.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
import boto3
import json

from strands_message_bus import (
    StrandsMessageBus, StrandsMessage, MessageType, get_message_bus
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgentState:
    """Agent state tracking."""
    INITIALIZING = "initializing"
    IDLE = "idle"
    PROCESSING = "processing"
    WAITING = "waiting"
    ERROR = "error"
    STOPPED = "stopped"


class StrandsAgent(ABC):
    """
    Base class for all Strands agents.
    Provides autonomous operation, message handling, and self-learning capabilities.
    """

    def __init__(self, agent_name: str, agent_type: str):
        self.agent_name = agent_name
        self.agent_type = agent_type
        self.state = AgentState.INITIALIZING
        self.message_bus: Optional[StrandsMessageBus] = None
        self.running = False

        # AWS clients
        self.bedrock = boto3.client('bedrock-runtime')
        self.s3 = boto3.client('s3')

        # Agent configuration
        self.config = self._load_agent_config()

        # Performance tracking
        self.metrics = {
            'messages_processed': 0,
            'errors': 0,
            'avg_processing_time': 0.0,
            'last_active': None
        }

        logger.info(f"Strands Agent '{self.agent_name}' ({self.agent_type}) initialized")

    def _load_agent_config(self) -> Dict[str, Any]:
        """Load agent-specific configuration."""
        # Override in subclasses to load custom config
        return {
            'max_concurrent_tasks': 5,
            'enable_learning': True,
            'bedrock_model': 'anthropic.claude-3-sonnet-20240229-v1:0',
            's3_bucket': 'strands-etl-learning'
        }

    async def start(self):
        """Start the agent's autonomous operation."""
        self.message_bus = get_message_bus()
        self.running = True
        self.state = AgentState.IDLE

        # Subscribe to messages
        self.message_bus.subscribe_to_agent(self.agent_name, self._handle_message)

        # Subscribe to specific message types
        for msg_type in self._get_subscribed_message_types():
            self.message_bus.subscribe_to_type(msg_type, self._handle_message)

        logger.info(f"Agent '{self.agent_name}' started and listening for messages")

        # Run agent-specific initialization
        await self.on_start()

        # Start autonomous operation loop
        await self._run()

    async def stop(self):
        """Stop the agent."""
        self.running = False
        self.state = AgentState.STOPPED
        await self.on_stop()
        logger.info(f"Agent '{self.agent_name}' stopped")

    async def _run(self):
        """Main agent execution loop."""
        while self.running:
            try:
                self.state = AgentState.IDLE

                # Perform autonomous tasks (e.g., periodic learning, cleanup)
                await self.autonomous_cycle()

                # Sleep briefly to prevent busy loop
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error in agent '{self.agent_name}' execution loop: {e}")
                self.state = AgentState.ERROR
                self.metrics['errors'] += 1

    async def _handle_message(self, message: StrandsMessage):
        """Handle incoming messages."""
        # Skip if message is from self or not targeted at this agent
        if message.sender_agent == self.agent_name:
            return

        if message.target_agent and message.target_agent != self.agent_name:
            return

        try:
            self.state = AgentState.PROCESSING
            start_time = datetime.utcnow()

            logger.info(f"Agent '{self.agent_name}' processing message {message.message_id} "
                       f"of type {message.message_type.value}")

            # Process the message
            response = await self.process_message(message)

            # Update metrics
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            self._update_metrics(processing_time)

            # Send response if provided
            if response:
                response_msg = self.message_bus.create_message(
                    sender=self.agent_name,
                    message_type=MessageType.AGENT_RESPONSE,
                    payload=response,
                    target=message.sender_agent,
                    correlation_id=message.correlation_id
                )
                await self.message_bus.publish(response_msg)

            self.state = AgentState.IDLE

        except Exception as e:
            logger.error(f"Error processing message in agent '{self.agent_name}': {e}")
            self.state = AgentState.ERROR
            self.metrics['errors'] += 1

            # Send error response
            error_msg = self.message_bus.create_message(
                sender=self.agent_name,
                message_type=MessageType.AGENT_ERROR,
                payload={'error': str(e), 'original_message_id': message.message_id},
                target=message.sender_agent,
                correlation_id=message.correlation_id
            )
            await self.message_bus.publish(error_msg)

    def _update_metrics(self, processing_time: float):
        """Update agent performance metrics."""
        self.metrics['messages_processed'] += 1
        self.metrics['last_active'] = datetime.utcnow().isoformat()

        # Update moving average of processing time
        count = self.metrics['messages_processed']
        current_avg = self.metrics['avg_processing_time']
        self.metrics['avg_processing_time'] = (
            (current_avg * (count - 1) + processing_time) / count
        )

    async def send_message(self,
                          target_agent: Optional[str],
                          message_type: MessageType,
                          payload: Dict[str, Any],
                          correlation_id: Optional[str] = None,
                          priority: int = 5):
        """Send a message to another agent or broadcast."""
        message = self.message_bus.create_message(
            sender=self.agent_name,
            message_type=message_type,
            payload=payload,
            target=target_agent,
            correlation_id=correlation_id,
            priority=priority
        )
        await self.message_bus.publish(message)
        logger.debug(f"Agent '{self.agent_name}' sent message {message.message_id}")

    async def invoke_bedrock(self, prompt: str, system_prompt: str = "") -> str:
        """Invoke AWS Bedrock for AI reasoning."""
        try:
            full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt

            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.bedrock.invoke_model(
                    modelId=self.config['bedrock_model'],
                    body=json.dumps({
                        'anthropic_version': 'bedrock-2023-05-31',
                        'max_tokens': 2000,
                        'messages': [
                            {
                                'role': 'user',
                                'content': full_prompt
                            }
                        ]
                    })
                )
            )

            result = json.loads(response['body'].read())
            return result['content'][0]['text']

        except Exception as e:
            logger.error(f"Bedrock invocation failed in agent '{self.agent_name}': {e}")
            return f"Error: {str(e)}"

    async def load_learning_vectors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Load learning vectors from S3 for informed decision-making."""
        try:
            bucket = self.config['s3_bucket']
            prefix = f'learning/vectors/{self.agent_type}/'

            # Use asyncio executor for S3 calls
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=limit
                )
            )

            vectors = []
            if 'Contents' in response:
                # Sort by last modified
                objects = sorted(response['Contents'],
                               key=lambda x: x['LastModified'],
                               reverse=True)

                # Parallel fetch using gather
                async def fetch_vector(obj_key):
                    try:
                        data = await asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: self.s3.get_object(Bucket=bucket, Key=obj_key)
                        )
                        return json.loads(data['Body'].read().decode('utf-8'))
                    except Exception as e:
                        logger.warning(f"Failed to load vector {obj_key}: {e}")
                        return None

                results = await asyncio.gather(*[
                    fetch_vector(obj['Key']) for obj in objects[:limit]
                ])

                vectors = [v for v in results if v is not None]

            return vectors

        except Exception as e:
            logger.error(f"Failed to load learning vectors: {e}")
            return []

    async def store_learning_vector(self, vector: Dict[str, Any]) -> bool:
        """Store a learning vector to S3."""
        try:
            bucket = self.config['s3_bucket']
            key = f"learning/vectors/{self.agent_type}/{vector.get('vector_id', 'unknown')}.json"

            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=json.dumps(vector, indent=2)
                )
            )

            logger.info(f"Learning vector stored: s3://{bucket}/{key}")
            return True

        except Exception as e:
            logger.error(f"Failed to store learning vector: {e}")
            return False

    # Abstract methods to be implemented by subclasses

    @abstractmethod
    async def process_message(self, message: StrandsMessage) -> Optional[Dict[str, Any]]:
        """Process an incoming message. Return response payload or None."""
        pass

    @abstractmethod
    async def autonomous_cycle(self):
        """Perform autonomous tasks during idle time."""
        pass

    @abstractmethod
    def _get_subscribed_message_types(self) -> List[MessageType]:
        """Return list of message types this agent subscribes to."""
        pass

    async def on_start(self):
        """Called when agent starts. Override for custom initialization."""
        pass

    async def on_stop(self):
        """Called when agent stops. Override for custom cleanup."""
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """Get agent performance metrics."""
        return {
            'agent_name': self.agent_name,
            'agent_type': self.agent_type,
            'state': self.state,
            **self.metrics
        }
