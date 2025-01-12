import requests

def openai_api_call(api_key, prompt,model='gpt-4o-mini'):
    # Set the endpoint URL and headers
    url='https://heisenberg-duckduckgo-66.deno.dev/v1/chat/completions'

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # Define the data payload
	# Modified
    data = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}]}

    # Make the request
    response = requests.post(url, headers=headers, json=data)

    # Check for a successful response
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.status_code, "message": response.text}
def main():

# Example usage
	api_key = "123456"
	prompt = "Translate the following English text to French: 'Hello, how are you?'"
	response = openai_api_call(api_key, prompt)

	if "error" in response:
	    print(f"Error: {response['message']}")
	else:
	# Modified
	    print(response["choices"][0]["message"]["content"].strip())
