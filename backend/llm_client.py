import asyncio

async def get_llm_response(prompt: str, model_name: str = "ollama-model"):
    # Simulate async call
    await asyncio.sleep(1)
    return f"[{model_name}] Echo: {prompt}"
