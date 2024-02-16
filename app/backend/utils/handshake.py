import os
import json
import hashlib
"""
This module is just a quick standin for testing purposes.
In production replace this with a secure database-backed
implementation.
"""

# File path for storing email and salt mappings
SALTS_FILE_PATH = 'email_salts.json'

def generate_salt():
    """Generate a random salt."""
    return os.urandom(16)

def hash_email_with_salt(email: str, salt: bytes) -> str:
    """Hash the email with the provided salt using PBKDF2."""
    dk = hashlib.pbkdf2_hmac('sha256', email.encode(), salt, 100000)
    return dk.hex()

def read_salts_from_file():
    """Read the email-salt mappings from the JSON file."""
    try:
        with open(SALTS_FILE_PATH, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def write_salt_to_file(email: str, salt: bytes):
    """Write the email-salt mapping to the JSON file."""
    salts = read_salts_from_file()
    salts[email] = salt.hex()  # Store salt as hex string for consistency
    with open(SALTS_FILE_PATH, 'w') as file:
        json.dump(salts, file)

def generate_client_id(email):
    # Generate a salt
    salt = os.urandom(16)
    # Hash email with salt
    client_id = hashlib.pbkdf2_hmac('sha256', email.encode(), salt, 100000)
    # Return the hashed client_id as a hex string for easier handling
    return client_id.hex()