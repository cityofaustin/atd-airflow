import time
from flask import Flask, request
import os, subprocess
import tempfile
import shutil
from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
VAULT_ID = os.getenv("OP_VAULT_ID")

SECRETS = {
    "webhook_token": {
        "opitem": "Airflow Webhook Token",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "deploy_key_private": {
        "opitem": "Airflow Deploy Key",
        "opfield": ".private key",
        "opvault": VAULT_ID,
    },
    "deploy_key_public": {
        "opitem": "Airflow Deploy Key",
        "opfield": ".public key",
        "opvault": VAULT_ID,
    }
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route('/')
def hello_world():
    return 'Airflow 2.5.3 `git pull` webhook @ ' + time.ctime()

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    client_key = request.headers.get('X-Webhook-Key')
    SECRET_VALUES = onepasswordconnectsdk.load_dict(client, SECRETS)
    if client_key == SECRET_VALUES["webhook_token"]:
        app.logger.info("successful webhook auth\n")
        with TempDir() as key_directory:
            write_to_file(key_directory + "/id_ed25519", SECRET_VALUES["deploy_key_private"] + "\n")
            os.chdir('/opt/airflow')
            environment = dict(os.environ)
            environment['GIT_SSH_COMMAND'] = f'ssh -i {key_directory}/id_ed25519 -o IdentitiesOnly=yes -o "StrictHostKeyChecking=no"'
            data = subprocess.run(['git', 'pull'], env=environment, cwd=r'/opt/airflow', check=True, stdout=subprocess.PIPE).stdout
            app.logger.info(data.decode('utf-8'))
            return 'git pull webhook received successfully'
    else:
        app.logger.info("failed webhook auth\n")
        return 'Failed to authenticate to webhook'

class TempDir:
    def __init__(self):
        self.path = None

    def __enter__(self):
        self.path = tempfile.mkdtemp(dir='/tmp')
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.path)

def write_to_file(path, content):
    # Open the file with write permissions and create it if it doesn't exist
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)

    # Write the content to the file
    os.write(fd, content.encode())

    # Close the file
    os.close(fd)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
