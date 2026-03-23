"""Shared utilities for LLM response parsing."""

import re


def repair_json(text: str) -> str:
    """Best-effort cleanup of malformed JSON from LLM responses.

    Handles common issues from smaller/chat models:
    - Preamble text before the JSON (e.g. "Here is the JSON response:")
    - Markdown code fences wrapping JSON (```json ... ```)
    - Trailing text/notes after the closing fence
    - Python-style True/False/None instead of true/false/null
    - Trailing commas before } or ]
    """
    s = text.strip()

    # 1. Extract content from a markdown code fence, anywhere in the string.
    #    Handles preamble ("Here is the JSON:") and trailing notes.
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", s, re.DOTALL)
    if fence_match:
        s = fence_match.group(1).strip()
    else:
        # 2. No fence — try to extract a bare JSON object { ... } or array [ ... ].
        #    Find the first { or [ and the last matching } or ].
        obj_match = re.search(r"(\{.*\}|\[.*\])", s, re.DOTALL)
        if obj_match:
            s = obj_match.group(1).strip()

    # Fix Python-style booleans/None (only outside quoted strings)
    # Simple approach: replace whole-word occurrences
    s = re.sub(r"\bTrue\b", "true", s)
    s = re.sub(r"\bFalse\b", "false", s)
    s = re.sub(r"\bNone\b", "null", s)

    # Remove trailing commas before } or ]
    s = re.sub(r",\s*([}\]])", r"\1", s)

    return s
