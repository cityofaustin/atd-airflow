"""
Various utilities for interacting with the onepasswordconnectsdk.
See: https://github.com/1Password/connect-sdk-python
"""
import os

from airflow.decorators import task
from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk
from pendulum import duration

ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
VAULT_ID = os.getenv("OP_VAULT_ID")


def get_client():
    """Get onepassword connect client

    Returns:
        Client
    """
    return new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)


def load_dict(required_secrets):
    """Return dict of specified keys and secrets from 1Password vault.

    Keyword arguments:
    required_secrets -- a dict of specified keys and values of location config w/o vault id
    """
    client = get_client()
    for value in required_secrets.values():
        value["opvault"] = VAULT_ID

    return onepasswordconnectsdk.load_dict(client, required_secrets)


def get_item_by_title(entry_name):
    """Return a specific item by item title and vault id.

    Keyword arguments:
    entry_name -- string value of the 1Password entry name in the vault
    """
    client = get_client()
    return client.get_item_by_title(entry_name, VAULT_ID)


@task(
    task_id="get_env_vars",
    execution_timeout=duration(seconds=30),
)
def get_env_vars_task(required_secrets):
    """This is a wrapper around `load_dict` so that it can be
    imported as an airflow task.

    Args:
        required_secrets (dict):  a dict of specified keys and values of location config
        w/o vault id

    Returns:
        dict: a dict with one key/val per secret
    """
    return load_dict(required_secrets)


@task(
    task_id="get_item_last_update_date",
    multiple_outputs=True,
    execution_timeout=duration(seconds=30),
)
def get_item_last_update_date(entry_name):
    """Return a specific item's last update date

    Keyword arguments:
    entry_name -- string value of the 1Password entry name in the vault
    """
    updated_at_datetime = get_item_by_title(entry_name).updated_at
    return {"updated_at": updated_at_datetime}
