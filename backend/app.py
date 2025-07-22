from fastapi import FastAPI, HTTPException
from .redis_client import MiniRedisClient
from .models import Message
import base64

app = FastAPI()
redis_client = MiniRedisClient()

@app.post("/chat/{chat_id}/send")
async def send_message(chat_id: str, message: Message):
    encoded_content = base64.b64encode(message.content.encode()).decode()
    cmd = f"LPUSH chat:{chat_id}:messages {message.sender}|{encoded_content}"
    resp = redis_client.send_command(cmd)
    if resp.startswith("Unknown"):
        raise HTTPException(status_code=400, detail=resp)
    return {"status": "message stored"}

@app.get("/chat/{chat_id}/messages")
async def get_messages(chat_id: str, count: int = 10):
    cmd = f"LRANGE chat:{chat_id}:messages 0 {count-1}"
    resp = redis_client.send_command(cmd)
    messages = resp.split('\n') if resp else []
    parsed = []
    print("[backend] Raw messages from Redis:", messages)  # raw list for debugging
    for m in messages:
        if '|' in m:
            sender, encoded_content = m.split('|', 1)
            try:
                content = base64.b64decode(encoded_content).decode()
            except Exception:
                content = encoded_content
            parsed.append({"sender": sender, "content": content})
            print(f"[backend] Decoded message: sender={sender}, content={content}")  # decoded print
    return {"messages": parsed}
