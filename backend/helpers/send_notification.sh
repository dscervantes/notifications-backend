#!/usr/bin/env bash
# Sends a notification to the local kafka environment using kafkacat
which kafkacat > /dev/null || (echo "kafkacat command is not available. Install it before continuing" && exit 1)
which jq > /dev/null || (echo "jq command is not available. Install it before continuing" && exit 1)

if [ -z "$ACCOUNT_ID" ]; then
  echo "ACCOUNT_ID is not set";
  exit 1
fi


BUNDLE='rhel'
APP='advisor'
EVENT_TYPE='new-recommendation'
TIMESTAMP=$(date --utc +%FT%TZ)

read -r -d '' EVENTS <<JSONDOC
[
  {
    "metadata": {},
    "payload": {
      "rule_id": "rule-id-001",
      "rule_description": "Sample recommendation",
      "total_risk": "2",
      "publish_date": "$(date --utc +%FT%TZ)",
      "rule_url": "http://example.com/rule-001"
    }
  }
]
JSONDOC

read -r -d ''  CONTEXT <<JSONDOC
{
  "inventory_id": "host-01",
  "hostname": "my-host",
  "display_name": "My system",
  "rhel_version": "8.3",
  "host_url": "http://example.com/host-01"
}
JSONDOC

read -r -d '' PAYLOAD <<JSONDOC
{
  "version": "v1.1.0",
  "bundle": "${BUNDLE}",
  "application": "${APP}",
  "event_type": "${EVENT_TYPE}",
  "timestamp": "${TIMESTAMP}",
  "account_id": "${ACCOUNT_ID}",
  "events": $(echo "${EVENTS}" | jq 'map(.payload |= (.. | tostring))'),
  "context": $(echo "${CONTEXT}" | jq '. | tostring'),
  "recipients": []
}
JSONDOC

echo "Sending notification:"
echo "$PAYLOAD" | jq '.'
echo "$PAYLOAD" | jq --compact-output '.' | kafkacat -P -t platform.notifications.ingress -b localhost:9092
