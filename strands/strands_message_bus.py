"""
Strands Message Bus - Event-driven communication system for independent agents.
Enables asynchronous, decoupled agent communication.
"""

import json
import asyncio
import logging
from typing import Dict, Any, Callable, List, Optional
from datetime import datetime
from enum import Enum
import uuid
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of messages that can be sent between agents."""
    AGENT_REQUEST = "agent_request"
    AGENT_RESPONSE = "agent_response"
    AGENT_ERROR = "agent_error"
    LEARNING_UPDATE = "learning_update"
    QUALITY_CHECK = "quality_check"
    OPTIMIZATION_RECOMMENDATION = "optimization_recommendation"
    DECISION_MADE = "decision_made"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"
    EXECUTION_FAILED = "execution_failed"


@dataclass
class StrandsMessage:
    """Message structure for inter-agent communication."""
    message_id: str
    message_type: MessageType
    sender_agent: str
    target_agent: Optional[str]  # None for broadcast
    payload: Dict[str, Any]
    timestamp: str
    correlation_id: str  # Links related messages in a workflow
    priority: int = 5  # 1-10, higher = more urgent

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        data = asdict(self)
        data['message_type'] = self.message_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrandsMessage':
        """Create message from dictionary."""
        data['message_type'] = MessageType(data['message_type'])
        return cls(**data)


class StrandsMessageBus:
    """
    Asynchronous message bus for Strands agents.
    Supports pub/sub, point-to-point, and broadcast messaging.
    """

    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = {}  # agent_name -> list of callbacks
        self.type_subscribers: Dict[MessageType, List[Callable]] = {}  # message_type -> callbacks
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.running = False
        self.message_history: List[StrandsMessage] = []
        self.max_history = 1000

    async def start(self):
        """Start the message bus processing loop."""
        self.running = True
        logger.info("Strands Message Bus started")
        await self._process_messages()

    async def stop(self):
        """Stop the message bus."""
        self.running = False
        logger.info("Strands Message Bus stopped")

    async def _process_messages(self):
        """Main message processing loop."""
        while self.running:
            try:
                # Wait for messages with timeout to allow clean shutdown
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                await self._route_message(message)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    async def _route_message(self, message: StrandsMessage):
        """Route message to appropriate subscribers."""
        logger.info(f"Routing message {message.message_id} from {message.sender_agent} "
                   f"to {message.target_agent or 'ALL'}")

        # Store in history
        self.message_history.append(message)
        if len(self.message_history) > self.max_history:
            self.message_history.pop(0)

        # Route to specific agent
        if message.target_agent and message.target_agent in self.subscribers:
            await self._notify_subscribers(self.subscribers[message.target_agent], message)

        # Route to type subscribers (broadcast to all interested in this type)
        if message.message_type in self.type_subscribers:
            await self._notify_subscribers(self.type_subscribers[message.message_type], message)

    async def _notify_subscribers(self, callbacks: List[Callable], message: StrandsMessage):
        """Notify all subscribers with the message."""
        tasks = []
        for callback in callbacks:
            try:
                # Support both async and sync callbacks
                if asyncio.iscoroutinefunction(callback):
                    tasks.append(callback(message))
                else:
                    # Run sync callbacks in executor
                    loop = asyncio.get_event_loop()
                    tasks.append(loop.run_in_executor(None, callback, message))
            except Exception as e:
                logger.error(f"Error in subscriber callback: {e}")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def publish(self, message: StrandsMessage):
        """Publish a message to the bus."""
        await self.message_queue.put(message)
        logger.debug(f"Message {message.message_id} published to bus")

    def subscribe_to_agent(self, agent_name: str, callback: Callable):
        """Subscribe to messages targeted at a specific agent."""
        if agent_name not in self.subscribers:
            self.subscribers[agent_name] = []
        self.subscribers[agent_name].append(callback)
        logger.info(f"Subscribed to agent '{agent_name}'")

    def subscribe_to_type(self, message_type: MessageType, callback: Callable):
        """Subscribe to all messages of a specific type."""
        if message_type not in self.type_subscribers:
            self.type_subscribers[message_type] = []
        self.type_subscribers[message_type].append(callback)
        logger.info(f"Subscribed to message type '{message_type.value}'")

    def create_message(self,
                       sender: str,
                       message_type: MessageType,
                       payload: Dict[str, Any],
                       target: Optional[str] = None,
                       correlation_id: Optional[str] = None,
                       priority: int = 5) -> StrandsMessage:
        """Create a new message."""
        return StrandsMessage(
            message_id=str(uuid.uuid4()),
            message_type=message_type,
            sender_agent=sender,
            target_agent=target,
            payload=payload,
            timestamp=datetime.utcnow().isoformat(),
            correlation_id=correlation_id or str(uuid.uuid4()),
            priority=priority
        )

    def get_message_history(self,
                           correlation_id: Optional[str] = None,
                           agent_name: Optional[str] = None,
                           limit: int = 100) -> List[StrandsMessage]:
        """Get message history with optional filtering."""
        filtered = self.message_history

        if correlation_id:
            filtered = [m for m in filtered if m.correlation_id == correlation_id]

        if agent_name:
            filtered = [m for m in filtered
                       if m.sender_agent == agent_name or m.target_agent == agent_name]

        return filtered[-limit:]


# Global message bus instance
_message_bus: Optional[StrandsMessageBus] = None


def get_message_bus() -> StrandsMessageBus:
    """Get or create the global message bus instance."""
    global _message_bus
    if _message_bus is None:
        _message_bus = StrandsMessageBus()
    return _message_bus


async def start_message_bus():
    """Start the global message bus."""
    bus = get_message_bus()
    if not bus.running:
        await bus.start()


async def stop_message_bus():
    """Stop the global message bus."""
    bus = get_message_bus()
    if bus.running:
        await bus.stop()
