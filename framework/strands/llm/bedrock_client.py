#!/usr/bin/env python3
"""
Bedrock Client for Strands ETL Framework
=========================================

Provides LLM capabilities via Amazon Bedrock with:
- Automatic retry with exponential backoff
- Graceful fallback to rule-based logic
- Token usage and cost tracking
- Response caching for repeated queries
"""

import json
import time
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, Callable, List
from functools import lru_cache

logger = logging.getLogger("strands.llm")


@dataclass
class LLMResponse:
    """Response from LLM invocation."""
    content: str
    model_id: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    cost_usd: float = 0.0
    cached: bool = False
    fallback_used: bool = False
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'content': self.content,
            'model_id': self.model_id,
            'input_tokens': self.input_tokens,
            'output_tokens': self.output_tokens,
            'latency_ms': self.latency_ms,
            'cost_usd': self.cost_usd,
            'cached': self.cached,
            'fallback_used': self.fallback_used,
            'error': self.error
        }


@dataclass
class UsageStats:
    """Accumulated usage statistics."""
    total_requests: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0
    total_latency_ms: float = 0.0
    cache_hits: int = 0
    fallback_count: int = 0
    error_count: int = 0


class BedrockClient:
    """
    Amazon Bedrock client with fallback support.

    Features:
    - Automatic retry with exponential backoff
    - Graceful fallback to rule-based logic on failure
    - Token usage tracking and cost estimation
    - Response caching for identical prompts
    """

    # Pricing per 1K tokens (Claude 3 Sonnet)
    PRICING = {
        'anthropic.claude-3-sonnet-20240229-v1:0': {'input': 0.003, 'output': 0.015},
        'anthropic.claude-3-haiku-20240307-v1:0': {'input': 0.00025, 'output': 0.00125},
        'anthropic.claude-3-5-sonnet-20240620-v1:0': {'input': 0.003, 'output': 0.015},
        'us.anthropic.claude-3-5-sonnet-20241022-v2:0': {'input': 0.003, 'output': 0.015},
    }

    DEFAULT_MODEL = 'us.anthropic.claude-3-5-sonnet-20241022-v2:0'

    def __init__(
        self,
        model_id: str = None,
        region: str = 'us-east-1',
        max_retries: int = 3,
        timeout_seconds: int = 60,
        enable_cache: bool = True
    ):
        """
        Initialize Bedrock client.

        Args:
            model_id: Bedrock model ID (default: Claude 3.5 Sonnet)
            region: AWS region
            max_retries: Max retry attempts on failure
            timeout_seconds: Request timeout
            enable_cache: Enable response caching
        """
        self.model_id = model_id or self.DEFAULT_MODEL
        self.region = region
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.enable_cache = enable_cache
        self._client = None
        self._cache: Dict[str, LLMResponse] = {}
        self._stats = UsageStats()

    @property
    def client(self):
        """Lazy-load Bedrock client."""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    'bedrock-runtime',
                    region_name=self.region
                )
            except Exception as e:
                logger.warning(f"Failed to create Bedrock client: {e}")
                raise
        return self._client

    def _get_cache_key(self, prompt: str, system: str = None) -> str:
        """Generate cache key for prompt."""
        content = f"{self.model_id}:{system or ''}:{prompt}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost based on token usage."""
        pricing = self.PRICING.get(self.model_id, {'input': 0.003, 'output': 0.015})
        input_cost = (input_tokens / 1000) * pricing['input']
        output_cost = (output_tokens / 1000) * pricing['output']
        return round(input_cost + output_cost, 6)

    def invoke(
        self,
        prompt: str,
        system: str = None,
        max_tokens: int = 4096,
        temperature: float = 0.0
    ) -> LLMResponse:
        """
        Invoke Bedrock model.

        Args:
            prompt: User prompt
            system: System prompt (optional)
            max_tokens: Maximum output tokens
            temperature: Sampling temperature (0.0 = deterministic)

        Returns:
            LLMResponse with content and usage stats
        """
        # Check cache first
        if self.enable_cache:
            cache_key = self._get_cache_key(prompt, system)
            if cache_key in self._cache:
                cached = self._cache[cache_key]
                self._stats.cache_hits += 1
                return LLMResponse(
                    content=cached.content,
                    model_id=self.model_id,
                    input_tokens=cached.input_tokens,
                    output_tokens=cached.output_tokens,
                    latency_ms=0.0,
                    cost_usd=0.0,  # No cost for cached response
                    cached=True
                )

        # Build request
        messages = [{"role": "user", "content": prompt}]
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": messages,
            "temperature": temperature
        }
        if system:
            request_body["system"] = system

        # Retry with exponential backoff
        last_error = None
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()

                response = self.client.invoke_model(
                    modelId=self.model_id,
                    body=json.dumps(request_body),
                    contentType='application/json',
                    accept='application/json'
                )

                latency_ms = (time.time() - start_time) * 1000
                response_body = json.loads(response['body'].read())

                # Extract content and usage
                content = response_body.get('content', [{}])[0].get('text', '')
                usage = response_body.get('usage', {})
                input_tokens = usage.get('input_tokens', 0)
                output_tokens = usage.get('output_tokens', 0)
                cost = self._calculate_cost(input_tokens, output_tokens)

                # Update stats
                self._stats.total_requests += 1
                self._stats.total_input_tokens += input_tokens
                self._stats.total_output_tokens += output_tokens
                self._stats.total_cost_usd += cost
                self._stats.total_latency_ms += latency_ms

                result = LLMResponse(
                    content=content,
                    model_id=self.model_id,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    latency_ms=latency_ms,
                    cost_usd=cost
                )

                # Cache successful response
                if self.enable_cache:
                    self._cache[cache_key] = result

                logger.debug(
                    f"LLM response: {input_tokens} in, {output_tokens} out, "
                    f"${cost:.4f}, {latency_ms:.0f}ms"
                )
                return result

            except Exception as e:
                last_error = str(e)
                self._stats.error_count += 1

                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    logger.warning(
                        f"Bedrock request failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Bedrock request failed after {self.max_retries} attempts: {e}")

        # All retries failed
        return LLMResponse(
            content='',
            model_id=self.model_id,
            error=last_error,
            fallback_used=True
        )

    def invoke_with_fallback(
        self,
        prompt: str,
        fallback_fn: Callable[[], Any],
        system: str = None,
        parse_fn: Callable[[str], Any] = None
    ) -> tuple[Any, LLMResponse]:
        """
        Invoke LLM with automatic fallback to rule-based function.

        Args:
            prompt: LLM prompt
            fallback_fn: Function to call if LLM fails
            system: System prompt
            parse_fn: Function to parse LLM response (if None, returns raw text)

        Returns:
            Tuple of (result, LLMResponse)
        """
        response = self.invoke(prompt, system)

        if response.error or not response.content:
            # LLM failed, use fallback
            logger.info("LLM failed, using rule-based fallback")
            self._stats.fallback_count += 1
            result = fallback_fn()
            response.fallback_used = True
            return result, response

        # Try to parse LLM response
        if parse_fn:
            try:
                result = parse_fn(response.content)
                return result, response
            except Exception as e:
                # Parse failed, use fallback
                logger.warning(f"Failed to parse LLM response: {e}. Using fallback.")
                self._stats.fallback_count += 1
                result = fallback_fn()
                response.fallback_used = True
                response.error = f"Parse error: {e}"
                return result, response

        return response.content, response

    def invoke_structured(
        self,
        prompt: str,
        system: str = None,
        expected_keys: List[str] = None
    ) -> tuple[Dict[str, Any], LLMResponse]:
        """
        Invoke LLM expecting a JSON response.

        Args:
            prompt: Prompt that should elicit JSON response
            system: System prompt
            expected_keys: Keys expected in the response

        Returns:
            Tuple of (parsed dict, LLMResponse)
        """
        # Add JSON instruction to prompt
        json_prompt = f"""{prompt}

IMPORTANT: Respond with valid JSON only. No markdown, no explanation, just the JSON object."""

        response = self.invoke(json_prompt, system)

        if response.error:
            return {}, response

        # Try to parse JSON
        try:
            # Handle potential markdown code blocks
            content = response.content.strip()
            if content.startswith('```'):
                lines = content.split('\n')
                content = '\n'.join(lines[1:-1])

            result = json.loads(content)

            # Validate expected keys
            if expected_keys:
                missing = [k for k in expected_keys if k not in result]
                if missing:
                    logger.warning(f"LLM response missing keys: {missing}")

            return result, response

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM JSON response: {e}")
            response.error = f"JSON parse error: {e}"
            return {}, response

    def get_stats(self) -> Dict[str, Any]:
        """Get accumulated usage statistics."""
        return {
            'total_requests': self._stats.total_requests,
            'total_input_tokens': self._stats.total_input_tokens,
            'total_output_tokens': self._stats.total_output_tokens,
            'total_cost_usd': round(self._stats.total_cost_usd, 4),
            'total_latency_ms': round(self._stats.total_latency_ms, 0),
            'cache_hits': self._stats.cache_hits,
            'fallback_count': self._stats.fallback_count,
            'error_count': self._stats.error_count,
            'avg_latency_ms': round(
                self._stats.total_latency_ms / max(1, self._stats.total_requests), 0
            )
        }

    def reset_stats(self) -> None:
        """Reset usage statistics."""
        self._stats = UsageStats()

    def clear_cache(self) -> None:
        """Clear response cache."""
        self._cache.clear()
