import os

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
VAULT_ID = os.getenv("OP_VAULT_ID")

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)


def load_dict(REQUIRED_SECRETS):
    # Add vault ID to each secret
    for value in REQUIRED_SECRETS.values():
        value["opvault"] = VAULT_ID

    return onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)
