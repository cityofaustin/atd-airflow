#!/bin/sh

WEBHOOK_PROXY_URL=$(op read op://$OP_VAULT_ID/Airflow\ Webhook/smee/address)
smee -u $WEBHOOK_PROXY_URL -t http://webhook:5000/webhook