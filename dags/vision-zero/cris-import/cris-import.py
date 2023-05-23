import os
import onepasswordconnectsdk
from airflow.decorators import dag, task
from onepasswordconnectsdk.client import Client, new_client
from datetime import datetime, timedelta
import logging

import re
import tempfile
import shutil
import psycopg2
import psycopg2.extras
import sysrsync
import hashlib
from sshtunnel import SSHTunnelForwarder

DEPLOYMENT_ENVIRONMENT = os.environ.get("ENVIRONMENT", 'development')   # our current environment from ['production', 'development']
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")                   # our secret to get secrets 🤐
ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")                      # where we get our secrets
VAULT_ID = os.getenv("OP_VAULT_ID")

# these temp directories are used to store ssh keys, because they will
# automatically clean themselves up when they go out of scope.
class SshKeyTempDir:
    def __init__(self):
        self.path = None

    def __enter__(self):
        self.path = tempfile.mkdtemp(dir='/tmp')
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.path)

def write_key_to_file(path, content):
    # Open the file with write permissions and create it if it doesn't exist
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)

    # Write the content to the file
    os.write(fd, content.encode())

    # Close the file
    os.close(fd)

@dag(
    dag_id="cris-import",
    description="Daily operation to download the latest CRIS data and import it into the database",
    schedule="0 7 * * *",
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0), # previous midnight
    catchup=False,
    tags=["vision-zero", "cris", "import"],
)
def cris_import():

    @task()
    def get_secrets():
        REQUIRED_SECRETS = {
            "SFTP_endpoint": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"Common.SFTP Endpoint",
                "opvault": VAULT_ID,
                },
            "sftp_endpoint_private_key": {
                "opitem": "SFTP Endpoint Key",
                "opfield": ".private key",
                "opvault": VAULT_ID,
                },
            "archive_extract_password": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": "Common.CRIS Archive Extract Password",
                "opvault": VAULT_ID,
                },
            "bastion_host": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Database Bastion",
                "opvault": VAULT_ID,
                },
            "bastion_ssh_username": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Bastion ssh Username",
                "opvault": VAULT_ID,
                },
            "database_host": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Database Host",
                "opvault": VAULT_ID,
                },
            "database_username": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Database Username",
                "opvault": VAULT_ID,
                },
            "database_password": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Database Password",
                "opvault": VAULT_ID,
                },
            "database_name": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Database Name",
                "opvault": VAULT_ID,
                },
            "database_ssl_policy": {
                "opitem": "Vision Zero CRIS Import",
                "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Database SSL Policy",
                "opvault": VAULT_ID,
                },
        }

        # instantiate a 1Password client
        client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)
        # get the requested secrets from 1Password
        SECRETS = onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)

        logger = logging.getLogger(__name__)
        logger.info("Secrets: " + str(SECRETS))

        return SECRETS

    @task()
    def download_archives(SECRETS):
        """
        Connect to the SFTP endpoint which receives archives from CRIS and
        download them into a temporary directory.

        Returns path of temporary directory as a string
        """

        logger = logging.getLogger(__name__)

        with SshKeyTempDir() as key_directory:
            logger.info("Key Directory: " + key_directory)
            write_key_to_file(key_directory + "/id_ed25519", SECRETS["sftp_endpoint_private_key"] + "\n") 

            # logger = prefect.context.get("logger")
            zip_tmpdir = tempfile.mkdtemp()
            logger.info("Zip Directory: " + zip_tmpdir)

            rsync = sysrsync.run(
                verbose=True,
                options=["-a"],
                source_ssh=SECRETS["SFTP_endpoint"],
                source="/home/txdot/*zip",
                sync_source_contents=False,
                destination=zip_tmpdir,
                private_key=key_directory + "/id_ed25519",
                strict_host_key_checking=False,
            )
            logger.info("Rsync return code: " + str(rsync.returncode))
            # check for a OS level return code of anything non-zero, which
            # would indicate to us that the child proc we kicked off didn't
            # complete successfully.
            # see: https://www.gnu.org/software/libc/manual/html_node/Exit-Status.html
            if rsync.returncode != 0:
                return False
            logger.info("Temp Directory: " + zip_tmpdir)
            return zip_tmpdir

    @task()
    def unzip_archives(SECRETS, archives_directory):
        """
        Unzips (and decrypts) archives received from CRIS

        Arguments: A path to a directory containing archives as a string

        Returns: A list of strings, each denoting a path to a folder
        containing an archive's contents
        """

        logger = logging.getLogger(__name__)

        extracted_csv_directories = []
        for filename in os.listdir(archives_directory):
            logger.info("About to unzip: " + filename + "with the command ...")
            extract_tmpdir = tempfile.mkdtemp()
            unzip_command = f'7za -y -p{SECRETS["archive_extract_password"]} -o"{extract_tmpdir}" x "{archives_directory}/{filename}"'
            logger.info(unzip_command)
            os.system(unzip_command)
            extracted_csv_directories.append(extract_tmpdir)
        logger.info("Here are the extracted CSV directories: " + str(extracted_csv_directories))
        return extracted_csv_directories

    @task()
    def group_csvs_into_logical_groups(extracted_archives_list, dry_run, secrets):

        logger = logging.getLogger(__name__)

        # ! We now have Airflow's more robust map/reduce functionality, so we can .expand() on the result
        # ! from an already expanded task. https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
        extracted_archives = extracted_archives_list[0]
        logger.info("Extracted Archive Location: " + str(extracted_archives))

        files = os.listdir(str(extracted_archives))
        logical_groups = []
        for file in files:
            if file.endswith(".xml"):
                continue
            match = re.search("^extract_(\d+_\d+)_", file)
            group_id = match.group(1)
            if group_id not in logical_groups:
                logical_groups.append(group_id)
        print("logical groups: " + str(logical_groups))
        map_safe_state = []
        for group in logical_groups:
            map_safe_state.append({
                "logical_group_id": group,
                "working_directory": str(extracted_archives),
                "csv_prefix": "extract_" + group + "_",
                "dry_run": dry_run,
                "secrets": secrets,
            })
        logger.info("Map Safe State: " + str(map_safe_state))
        return map_safe_state

    @task()
    def create_import_schema_name(mapped_state):

        logger = logging.getLogger(__name__)

        print(mapped_state)
        schema = 'import_' + hashlib.md5(mapped_state["logical_group_id"].encode()).hexdigest()[:12]
        mapped_state["import_schema"] = schema
        logger.info("Schema name: " + mapped_state["import_schema"])
        return mapped_state

    @task()
    def create_target_import_schema(map_state):

        logger = logging.getLogger(__name__)  
        logger.info("Shhh" + str(map_state["secrets"]))

        DB_BASTION_HOST = map_state["secrets"]["bastion_host"]
        DB_BASTION_HOST_SSH_USERNAME = map_state["secrets"]["bastion_ssh_username"]
        DB_RDS_HOST = map_state["secrets"]["database_host"]
        DB_USER = map_state["secrets"]["database_username"]
        DB_PASS = map_state["secrets"]["database_password"]
        DB_NAME = map_state["secrets"]["database_name"]
        DB_SSL_REQUIREMENT = map_state["secrets"]["database_ssl_policy"]

        ssh_tunnel = SSHTunnelForwarder(
            (DB_BASTION_HOST),
            ssh_username=DB_BASTION_HOST_SSH_USERNAME,
            #ssh_private_key= '/root/.ssh/id_rsa', # will switch to ed25519 when we rebuild this for prefect 2
            remote_bind_address=(DB_RDS_HOST, 5432)
            )
        ssh_tunnel.start()   

        pg = psycopg2.connect(
            host='localhost', 
            port=ssh_tunnel.local_bind_port,
            user=DB_USER, 
            password=DB_PASS, 
            dbname=DB_NAME, 
            sslmode=DB_SSL_REQUIREMENT, 
            sslrootcert="/root/rds-combined-ca-bundle.pem"
            )

        cursor = pg.cursor()
        
        # check if the schema exists by querying the pg_namespace system catalog
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '{map_state['import_schema']}')")

        schema_exists = cursor.fetchone()[0]

        # if the schema doesn't exist, create it using a try-except block to handle the case where it already exists
        if not schema_exists:
            try:
                cursor.execute(f"CREATE SCHEMA {map_state['import_schema']}")
                print("Schema created successfully")
            except psycopg2.Error as e:
                print(f"Error creating schema: {e}")
        else:
            print("Schema already exists")

        # commit the changes and close the cursor and connection
        pg.commit()
        cursor.close()
        pg.close()

        return map_state


    dry_run = True

    secrets = get_secrets()
    archive_location = download_archives(secrets)
    extracted_archives = unzip_archives(secrets, archive_location)
    logical_groups_of_csvs = group_csvs_into_logical_groups(extracted_archives, dry_run, secrets)
    desired_schema_name = create_import_schema_name.expand(mapped_state=logical_groups_of_csvs)
    schema_name = create_target_import_schema.expand(map_state=desired_schema_name)



cris_import()