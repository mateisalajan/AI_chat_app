import httpx
import asyncio
import json

OLLAMA_API_URL = "http://localhost:11434"

async def call_ollama_model(prompt: str, model_name: str = "llama3.2:1b") -> str:
    full_response = ""
    async with httpx.AsyncClient() as client:
        async with client.stream(
            "POST",
            f"{OLLAMA_API_URL}/api/generate",
            json={"model": model_name, "prompt": prompt},
            timeout=None
        ) as response:
            async for line in response.aiter_lines():
                if line:
                    data = json.loads(line)
                    full_response += data.get("response", "")
                    if data.get("done", False):
                        break
    return full_response
