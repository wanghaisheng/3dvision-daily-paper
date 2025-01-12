import asyncio
import random
import requests
import json

STATUS_URL = "https://duckduckgo.com/duckchat/v1/status"
CHAT_URL = "https://duckduckgo.com/duckchat/v1/chat"
STATUS_HEADERS = {"x-vqd-accept": "1"}

# Models Mapping
MODEL_MAP = {
    "gpt-4o-mini": "gpt-4o-mini",
    "claude-3-haiku": "claude-3-haiku-20240307",
    "llama": "meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo",
    "mixtral": "mistralai/Mixtral-8x7B-Instruct-v0.1",
}

class Chat:
    def __init__(self, vqd: str, model: str):
        self.old_vqd = vqd
        self.new_vqd = vqd
        self.model = model
        self.messages = []

    def fetch(self, content: str) -> requests.Response:
        """ Send a message to the chat API. """
        self.messages.append({"content": content, "role": "user"})
        payload = {
            "model": self.model,
            "messages": self.messages,
        }
        headers = {"x-vqd-4": self.new_vqd, "Content-Type": "application/json"}
        response = requests.post(CHAT_URL, headers=headers, json=payload)

        if not response.ok:
            raise Exception(f"{response.status_code}: Failed to send message. {response.text}")
        return response

    def fetch_full(self, content: str) -> str:
        """ Fetch the full message (waiting for completion). """
        response = self.fetch(content)
        text = ""
        # Handle streaming response from the server
        for event in response.iter_lines():
            if event and event != b"[DONE]":
                message_data = json.loads(event.decode("utf-8"))
                message = message_data.get("message", "")
                if message:
                    text += message
        # Update the vqd
        new_vqd = response.headers.get("x-vqd-4")
        self.old_vqd = self.new_vqd
        self.new_vqd = new_vqd
        self.messages.append({"content": text, "role": "assistant"})
        return text

    async def fetch_stream(self, content: str) -> str:
        """ Fetch the message in a streaming fashion. """
        response = self.fetch(content)
        text = ""
        # Handle streaming response from the server
        for event in response.iter_lines():
            if event:
                message_data = json.loads(event.decode("utf-8"))
                message = message_data.get("message", "")
                if message:
                    text += message
                    yield message
        # Update the vqd
        new_vqd = response.headers.get("x-vqd-4")
        self.old_vqd = self.new_vqd
        self.new_vqd = new_vqd
        self.messages.append({"content": text, "role": "assistant"})

    def redo(self):
        """ Revert to the previous state. """
        self.new_vqd = self.old_vqd
        self.messages.pop()
        self.messages.pop()

async def init_chat(model_alias: str) -> Chat:
    """ Initialize a chat with the given model alias. """
    status = requests.get(STATUS_URL, headers=STATUS_HEADERS)
    vqd = status.headers.get("x-vqd-4")
    if not vqd:
        raise Exception(f"{status.status_code}: Failed to initialize chat. {status.text}")
    model = MODEL_MAP.get(model_alias)
    if not model:
        raise ValueError("Invalid model alias")
    return Chat(vqd, model)

# Example Usage:
async def example_usage():
    chat = await init_chat("gpt-4o-mini")
    response = await chat.fetch_full("Hello, how are you?")
    print(response)

# Run example usage
if __name__ == "__main__":
    asyncio.run(example_usage())
