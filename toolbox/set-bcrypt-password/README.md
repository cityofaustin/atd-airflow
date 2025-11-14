# Set BCrypt Password

Sets or updates the password for any user in Airflow's Simple Auth Manager.

## Description

This script creates or updates the password file used by Airflow's Simple Auth Manager. It securely hashes passwords using bcrypt and stores them in the JSON format expected by Airflow. The script can be used to set passwords for any user configured in your `simple_auth_manager_users` setting.

The script is designed to be run from a virtual environment (venv) on your host machine, and outputs the password file to the root of the repository. This file is then bind mounted into the Airflow containers.

## Prerequisites

1. **Activate your virtual environment:**
   ```bash
   source venv/bin/activate
   ```

2. **Ensure `bcrypt` is installed in your venv:**
   ```bash
   pip install bcrypt
   ```

## Usage

**Basic usage (prompts for password, defaults to 'admin' user, outputs to repo root):**
```bash
source venv/bin/activate
python toolbox/set-bcrypt-password/set_bcrypt_password.py admin ./simple_auth_manager_passwords.json
```

**Specify username (outputs to repo root):**
```bash
source venv/bin/activate
python toolbox/set-bcrypt-password/set_bcrypt_password.py admin ./simple_auth_manager_passwords.json
python toolbox/set-bcrypt-password/set_bcrypt_password.py viewer ./simple_auth_manager_passwords.json
python toolbox/set-bcrypt-password/set_bcrypt_password.py myuser ./simple_auth_manager_passwords.json
```

**Specify username and custom password file path:**
```bash
source venv/bin/activate
python toolbox/set-bcrypt-password/set_bcrypt_password.py admin /path/to/passwords.json
```

## Configuration

The script should be run with the password file path pointing to the root of the repository:
- **Password file location**: `./simple_auth_manager_passwords.json` (in the repo root)

This file is bind mounted into the Airflow containers at:
- `/opt/airflow/simple_auth_manager_passwords.json` (inside the container)

**Important:** Make sure your `docker-compose.yaml` includes a volume mount for the password file from the repo root to `/opt/airflow/simple_auth_manager_passwords.json` in the containers.

## Configuring Airflow to Use the Password File

To make Airflow use your custom password file, you need to configure the `simple_auth_manager_passwords_file` setting in your `airflow.cfg`:

### Option 1: Using airflow.cfg

Edit your `config/airflow.cfg` file and set:

```ini
[core]
auth_manager = airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
simple_auth_manager_users = admin:admin
simple_auth_manager_all_admins = False
simple_auth_manager_passwords_file = /opt/airflow/simple_auth_manager_passwords.json
```

### Option 2: Using Environment Variable

You can also set this via environment variable in your `docker-compose.yaml`:

```yaml
environment:
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE: /opt/airflow/simple_auth_manager_passwords.json
```

### Important Notes

- The path must be an **absolute path** (starting with `/`)
- Make sure the file exists and is readable by the Airflow process
- The file should contain a JSON object with usernames as keys and bcrypt-hashed passwords as values
- After updating the configuration, **restart your Airflow webserver** for the changes to take effect

## Security Notes

- The script uses `getpass` to securely prompt for passwords (input is hidden)
- Passwords are hashed using bcrypt before storage
- The password file should be kept secure and not committed to version control
- Make sure the password file has appropriate file permissions (readable only by Airflow user)
- Consider adding the password file to your `.gitignore`:
  ```
  simple_auth_manager_passwords.json
  ```

## Requirements

- Python 3.x
- Virtual environment (venv) with `bcrypt` library installed
  - Install with: `pip install bcrypt`

## Examples

**Setting password for admin user:**
```bash
$ source venv/bin/activate
$ python toolbox/set-bcrypt-password/set_bcrypt_password.py admin ./simple_auth_manager_passwords.json
Setting password for user: admin
Enter password: 
Confirm password: 
Password file created/updated at: ./simple_auth_manager_passwords.json
Password for user 'admin' has been set.
```

**Setting password for a different user:**
```bash
$ source venv/bin/activate
$ python toolbox/set-bcrypt-password/set_bcrypt_password.py myuser ./simple_auth_manager_passwords.json
Setting password for user: myuser
Enter password: 
Confirm password: 
Password file created/updated at: ./simple_auth_manager_passwords.json
Password for user 'myuser' has been set.
```

**Setting password with custom file path:**
```bash
$ source venv/bin/activate
$ python toolbox/set-bcrypt-password/set_bcrypt_password.py admin /custom/path/passwords.json
Setting password for user: admin
Enter password: 
Confirm password: 
Password file created/updated at: /custom/path/passwords.json
Password for user 'admin' has been set.
```

## Related Configuration

Make sure your `airflow.cfg` has the following settings:

```ini
[core]
auth_manager = airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
simple_auth_manager_users = admin:admin,viewer:viewer
simple_auth_manager_all_admins = False
simple_auth_manager_passwords_file = /opt/airflow/simple_auth_manager_passwords.json
```

After setting the password and updating the configuration, restart your Airflow webserver for the changes to take effect.
