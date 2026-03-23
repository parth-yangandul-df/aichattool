"""OpenRouter LLM provider — OpenAI-compatible API with 300+ models.

OpenRouter exposes the OpenAI chat-completions interface, so we subclass
OpenAIProvider and only change:
  • base_url → https://openrouter.ai/api/v1
  • two required headers: HTTP-Referer and X-Title
  • provider_type → LLMProviderType.OPENROUTER
  • list_models() → OpenRouter model catalogue

Embeddings: OpenRouter does not offer an embeddings endpoint.
The embedding_service falls back to OpenAI when the provider is openrouter
(same logic already used for Anthropic).

Usage (.env):
    DEFAULT_LLM_PROVIDER=openrouter
    OPENROUTER_API_KEY=sk-or-...
    OPENROUTER_MODEL=openai/gpt-3.5-turbo
    EMBEDDING_DIMENSION=1536
    OPENAI_API_KEY=<your-key>   # still required for embeddings
"""

from collections.abc import AsyncIterator

import openai

from app.config import settings
from app.llm.base_provider import (
    LLMConfig,
    LLMMessage,
    LLMProviderType,
    LLMResponse,
)
from app.llm.providers.openai_provider import OpenAIProvider

_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


class OpenRouterProvider(OpenAIProvider):
    """Thin wrapper around OpenAIProvider pointed at OpenRouter."""

    provider_type = LLMProviderType.OPENROUTER

    def __init__(self, api_key: str | None = None) -> None:
        resolved_key = api_key or settings.openrouter_api_key

        # OpenRouter requires two extra headers per their docs
        default_headers = {
            "HTTP-Referer": "https://github.com/querywise/querywise",
            "X-Title": "QueryWise",
        }

        self._client = openai.AsyncOpenAI(
            api_key=resolved_key,
            base_url=_OPENROUTER_BASE_URL,
            default_headers=default_headers,
            timeout=60.0,
        )

    # complete() and stream() are inherited from OpenAIProvider unchanged.
    # OpenRouter returns the same response shape as OpenAI.

    async def generate_embedding(self, text: str) -> list[float]:
        """OpenRouter has no embeddings endpoint — raise a clear error.

        In practice this is never called: provider_registry.get_embedding_provider()
        maps 'openrouter' → 'openai' (same as 'anthropic'), so embeddings always
        go through the OpenAI provider directly.
        """
        raise NotImplementedError(
            "OpenRouter does not provide an embeddings endpoint. "
            "Set OPENAI_API_KEY so embeddings can fall back to OpenAI."
        )

    async def stream(
        self,
        messages: list[LLMMessage],
        config: LLMConfig,
    ) -> AsyncIterator[str]:
        # Delegate to parent — identical wire format
        async for token in super().stream(messages, config):
            yield token

    async def complete(
        self,
        messages: list[LLMMessage],
        config: LLMConfig,
    ) -> LLMResponse:
        return await super().complete(messages, config)

    def list_models(self) -> list[str]:
        return [
            "openai/gpt-3.5-turbo",
            "openai/gpt-4o",
            "openai/gpt-4o-mini",
            "anthropic/claude-3-haiku",
            "anthropic/claude-3-sonnet",
            "meta-llama/llama-3.1-8b-instruct",
            "mistralai/mistral-7b-instruct",
        ]
