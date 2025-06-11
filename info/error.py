import json

# Check if file exists
try:
    with open('config.json', 'r') as f:
        content = f.read()
    print(f"File found! Size: {len(content)} characters")
    print(f"First 50 chars: {content[:50]}")
except FileNotFoundError:
    print("File config.json not found!")
    exit()

# Try to parse JSON
try:
    data = json.loads(content)
    print(f"JSON is valid! Found {len(data)} items")
    print(f"First item keys: {list(data[0].keys()) if data else 'No items'}")
except json.JSONDecodeError as e:
    print(f"JSON error: {e}")
    print(f"Error at position {e.pos}")
    # Show the problematic part
    start = max(0, e.pos - 20)
    end = min(len(content), e.pos + 20)
    print(f"Around error: {content[start:end]}")
except Exception as e:
    print(f"Other error: {e}")