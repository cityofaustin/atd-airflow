#!/usr/bin/env python3
"""
Script to set or update passwords for users in Airflow's Simple Auth Manager.
This creates/updates the password file with bcrypt-hashed passwords.
"""
import json
import bcrypt
import os
import sys
import getpass
from pathlib import Path

def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    # Generate a salt and hash the password
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def create_password_file(username: str, password: str, password_file_path: str):
    """Create or update the password file for Simple Auth Manager."""
    # Hash the password
    password_hash = hash_password(password)
    
    # Create the password data structure
    password_data = {
        username: password_hash
    }
    
    # If file exists, read and update it
    if os.path.exists(password_file_path):
        with open(password_file_path, 'r') as f:
            existing_data = json.load(f)
        existing_data.update(password_data)
        password_data = existing_data
    
    # Write the password file
    os.makedirs(os.path.dirname(password_file_path), exist_ok=True)
    with open(password_file_path, 'w') as f:
        json.dump(password_data, f, indent=2)
    
    print(f"Password file created/updated at: {password_file_path}")
    print(f"Password for user '{username}' has been set.")

if __name__ == "__main__":
    # Default username
    username = "admin"
    
    # Get password file path
    password_file = os.environ.get(
        "AIRFLOW_HOME",
        "."
    ) + "/simple_auth_manager_passwords.json"
    
    # Allow override via command line arguments
    if len(sys.argv) > 1:
        username = sys.argv[1]
    if len(sys.argv) > 2:
        password_file = sys.argv[2]
    
    # Prompt for password (hidden input)
    print(f"Setting password for user: {username}")
    password = getpass.getpass("Enter password: ")
    password_confirm = getpass.getpass("Confirm password: ")
    
    # Verify passwords match
    if password != password_confirm:
        print("Error: Passwords do not match!", file=sys.stderr)
        sys.exit(1)
    
    # Check password is not empty
    if not password:
        print("Error: Password cannot be empty!", file=sys.stderr)
        sys.exit(1)
    
    create_password_file(username, password, password_file)

