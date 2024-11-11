import requests
import concurrent.futures
import json

# Define the API endpoint
url = 'http://localhost:3000/api/v1/comments'

# Function to send a single POST request
def send_request(message, comment_type):
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({
        "text": message,
        "type": comment_type
    })
    try:
        response = requests.post(url, headers=headers, data=data)
        print(f"Sent message '{message}' of type {comment_type}, Response: {response.status_code}")
        return response.status_code
    except Exception as e:
        print(f"Failed to send message '{message}' of type {comment_type}, Error: {e}")
        return None

# List of messages and types to be sent
messages = [
    {"text": "message 1", "type": 1},
    # {"text": "message 2", "type": 2},
    # {"text": "message 3", "type": 3},
    # Add more messages if needed
]

# Number of concurrent requests to send
concurrent_requests = 100

# Main function to execute the requests concurrently
def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        futures = [
            executor.submit(send_request, msg["text"], msg["type"])
            for msg in messages
            for _ in range(concurrent_requests)  # Duplicate each message for multiple requests
        ]
        # Wait for all futures to complete
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    main()
