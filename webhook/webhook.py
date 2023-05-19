import sys
import time
import json
import time
from flask import Flask, request
import os, subprocess
from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
VAULT_ID = "quvhrzaatbj2wotsjrumx3f62a"
SECRET_NAME = "airflow.fyi webhook key"

SECRETS = {
    "secret_value": {
    "opitem": SECRET_NAME,
    "opfield": ".password",
    "opvault": VAULT_ID,
    },
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route('/')
def hello_world():
    return 'Airflow 2.5.3 `git pull` webhook @ ' + time.ctime()

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    payload = request.get_json()
    SECRET_VALUES = onepasswordconnectsdk.load_dict(client, SECRETS)

    if payload["data"]["webhook_key"] == SECRET_VALUES["secret_value"]:
        sys.stderr.write("successful webhook auth\n")
        os.chdir('/opt/airflow')
        environment = dict(os.environ)
        environment['GIT_SSH_COMMAND'] = 'ssh -i /opt/private_key_for_github -o IdentitiesOnly=yes -o "StrictHostKeyChecking=no"'
        data = subprocess.run(['git', 'pull'], env=environment, cwd=r'/opt/airflow', check=True, stdout=subprocess.PIPE).stdout
        return 'Received webhook payload'
    else:
        sys.stderr.write("failed to auth\n")
        return 'Failed to authenticate to webhook'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
