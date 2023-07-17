import os

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
VAULT_ID = os.getenv("OP_VAULT_ID")

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)


def load_dict(required_secrets):
    """Return dict of specified keys and secrets from 1Password vault.

    Keyword arguments:
    required_secrets -- a dict of specified keys and values of location config w/o vault id
    """
    for value in required_secrets.values():
        value["opvault"] = VAULT_ID

    return onepasswordconnectsdk.load_dict(client, required_secrets)


def get_item_by_title(entry_name):
    """Return a specific item by item title and vault id.

    Keyword arguments:
    entry_name -- string value of the 1Password entry name in the vault
    """
    return client.get_item_by_title(entry_name, VAULT_ID)
