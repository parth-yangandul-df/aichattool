import time
from collections.abc import AsyncIterator

import openai

from app.llm.base_provider import (
    BaseLLMProvider,
    LLMConfig,
    LLMMessage,
    LLMProviderType,
    LLMResponse,
)


class OpenAIProvider(BaseLLMProvider):
    provider_type = LLMProviderType.OPENAI

    def __init__(self, api_key: str | None = None):
        self._client = openai.AsyncOpenAI(api_key=api_key, timeout=30.0)

    async def complete(
        self,
        messages: list[LLMMessage],
        config: LLMConfig,
    ) -> LLMResponse:
        oai_messages = [{"role": m.role, "content": m.content} for m in messages]

        start = time.monotonic()
        response = await self._client.chat.completions.create(
            model=config.model,
            messages=oai_messages,
            temperature=config.temperature,
            max_completion_tokens=config.max_tokens,
            top_p=config.top_p,
            stop=config.stop_sequences or None,
        )
        elapsed_ms = (time.monotonic() - start) * 1000

        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            input_tokens=response.usage.prompt_tokens if response.usage else 0,
            output_tokens=response.usage.completion_tokens if response.usage else 0,
            finish_reason=choice.finish_reason or "stop",
            latency_ms=elapsed_ms,
        )

    async def stream(
        self,
        messages: list[LLMMessage],
        config: LLMConfig,
    ) -> AsyncIterator[str]:
        oai_messages = [{"role": m.role, "content": m.content} for m in messages]

        stream = await self._client.chat.completions.create(
            model=config.model,
            messages=oai_messages,
            temperature=config.temperature,
            max_completion_tokens=config.max_tokens,
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    async def generate_embedding(self, text: str) -> list[float]:
        response = await self._client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )
        return response.data[0].embedding

    def list_models(self) -> list[str]:
        return [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4-turbo",
        ]
