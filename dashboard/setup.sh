#!/bin/sh

# Function to check if Grafana is ready
is_grafana_ready() {
    curl --output /dev/null --silent --head --fail http://grafana:3000/api/health
}

# Wait for Grafana to be fully ready
echo "Waiting for Grafana to be fully ready..."
until is_grafana_ready; do
    echo "Grafana is not ready yet..."
    sleep 5
done

echo "Grafana is up and running."

# Generate a unique API key name
API_KEY_NAME="Automation API Key $(date +%s)"

# Generate API key
API_KEY_RESPONSE=$(curl -X POST http://grafana:3000/api/auth/keys \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$API_KEY_NAME\",\"role\":\"Admin\"}" \
    -u admin:admin)

echo "API Key Response: $API_KEY_RESPONSE"

API_KEY=$(echo $API_KEY_RESPONSE | jq -r .key)

if [ "$API_KEY" = "null" ]; then
  echo "Failed to generate API key: $API_KEY_RESPONSE"
  exit 1
fi

echo "Generated API key: $API_KEY"

# Use the generated API key to configure Grafana
python create_dashboard.py $API_KEY