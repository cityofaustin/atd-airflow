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
OPERATING_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

SECRETS = {
    "webhook_token": {
        "opitem": "Airflow Webhook",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "deploy_key_private": {
        "opitem": "Airflow Deploy Key",
        "opfield": ".private key",
        "opvault": VAULT_ID,
    }
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route('/')
def status():
    return 'Airflow 2.5.3 `git pull` webhook @ ' + time.ctime()

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    # we only want to run this in production, so we check the environment variable
    if OPERATING_ENVIRONMENT != "production":
        app.logger.info("\nnot in production, skipping webhook\n")
        return "Not in production, skipping webhook"
    # get the client key from the request header
    client_key = request.headers.get('X-Webhook-Key')
    # get the secret values from 1Password
    SECRET_VALUES = onepasswordconnectsdk.load_dict(client, SECRETS)
    # check if the the access token matches
    if client_key == SECRET_VALUES["webhook_token"]:
        app.logger.info("successful webhook auth\n")
        # create a directory to store the deploy key stored in 1password
        # this directory is automatically cleaned up when the next block of code finishes
        with TempDir() as key_directory:
            # write the deploy key to a file, trailing new line required! POSIX standard 
            write_to_file(key_directory + "/id_ed25519", SECRET_VALUES["deploy_key_private"] + "\n") 
            # change directory to the checkout location
            os.chdir('/opt/airflow')
            # create an environment object, so we have one to add a value to
            environment = dict(os.environ)
            # set the GIT_SSH_COMMAND environment variable to use the deploy key
            # StrictHostKeyChecking is turned off so you don't get the "do you want to trust this host?" prompt
            environment['GIT_SSH_COMMAND'] = f'ssh -i {key_directory}/id_ed25519 -o IdentitiesOnly=yes -o "StrictHostKeyChecking=no"'
            # pull the latest changes from the git repository
            data = subprocess.run(['git', 'pull'], env=environment, cwd=r'/opt/airflow', check=True, stdout=subprocess.PIPE).stdout
            # print the output to the logger
            app.logger.info(data.decode('utf-8'))
            return 'git pull webhook received successfully'
    # if the access token doesn't match, return an error
    else:
        app.logger.info("failed webhook auth\n")
        return 'Failed to authenticate to webhook'


def write_to_file(path, content):
    # Open the file with write permissions and create it if it doesn't exist
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)

    # Write the content to the file
    os.write(fd, content.encode())

    # Close the file
    os.close(fd)

# Class which will create a temporary directory, 
# return the path, and then delete it when it goes out of scope
class TempDir:
    def __init__(self):
        self.path = None

    def __enter__(self):
        self.path = tempfile.mkdtemp(dir='/tmp')
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.path)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
