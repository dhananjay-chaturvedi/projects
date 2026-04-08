#-------------------------------------------------------------------------------
#description: Connection monitor manager for the tool
#initial version: 08-APR-2026
#Author: Dhananjay Chaturvedi
#Copyright 2026 Dhananjay Chaturvedi
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#-------------------------------------------------------------------------------

import json
import os
import sys
from pathlib import Path
from cryptography.fernet import Fernet
import base64
from config_loader import config

class MonitorConnectionManager:
    """Manage saved server monitoring connections with encrypted passwords"""

    def __init__(self, config_file=None):
        self.config_dir = config.get_path('paths', 'config_dir')
        self.config_dir.mkdir(exist_ok=True)

        if config_file is None:
            config_file = config.get('paths', 'saved_monitor_connections_file', 'saved_monitor_connections.json')
        self.config_file = self.config_dir / config_file

        key_file_name = config.get('paths', 'monitor_key_file', '.monitor_key')
        self.key_file = self.config_dir / key_file_name

        # Initialize encryption
        self.cipher = self._init_cipher()

        self.connections = self.load_connections()

    def _init_cipher(self):
        """Initialize encryption cipher with key"""
        # Load or generate encryption key
        if self.key_file.exists():
            with open(self.key_file, 'rb') as f:
                key = f.read()
        else:
            # Generate new key
            key = Fernet.generate_key()
            with open(self.key_file, 'wb') as f:
                f.write(key)
            # Secure the key file (readable only by owner)
            file_perms = config.get_octal('security', 'key_file_permissions', default=0o600)
            os.chmod(self.key_file, file_perms)

        return Fernet(key)

    def _encrypt_password(self, password):
        """Encrypt a password"""
        if not password:
            return None
        try:
            encrypted = self.cipher.encrypt(password.encode())
            return base64.b64encode(encrypted).decode('utf-8')
        except Exception as e:
            print(f"Error encrypting password: {e}", file=sys.stderr)
            return None

    def _decrypt_password(self, encrypted_password):
        """Decrypt a password"""
        if not encrypted_password:
            return None
        try:
            encrypted_bytes = base64.b64decode(encrypted_password.encode('utf-8'))
            decrypted = self.cipher.decrypt(encrypted_bytes)
            return decrypted.decode('utf-8')
        except Exception as e:
            print(f"Error decrypting password: {e}", file=sys.stderr)
            return None

    def load_connections(self):
        """Load saved monitor connections from file and decrypt passwords"""
        if not self.config_file.exists():
            return []

        try:
            with open(self.config_file, 'r') as f:
                connections = json.load(f)

            # Decrypt passwords
            for conn in connections:
                if conn.get('password'):
                    conn['password'] = self._decrypt_password(conn['password'])

            return connections
        except Exception as e:
            print(f"Error loading monitor connections: {e}", file=sys.stderr)
            return []

    def save_connections(self):
        """Save monitor connections to file with encrypted passwords"""
        try:
            # Create a copy to encrypt passwords without modifying originals
            connections_to_save = []
            for conn in self.connections:
                conn_copy = conn.copy()
                if conn_copy.get('password'):
                    conn_copy['password'] = self._encrypt_password(conn_copy['password'])
                connections_to_save.append(conn_copy)

            with open(self.config_file, 'w') as f:
                json.dump(connections_to_save, f, indent=2)

            # Secure the connections file (readable only by owner)
            file_perms = config.get_octal('security', 'config_file_permissions', default=0o600)
            os.chmod(self.config_file, file_perms)

            return True
        except Exception as e:
            print(f"Error saving monitor connections: {e}", file=sys.stderr)
            return False

    def add_connection(self, name, host, username, password=None):
        """Add a new monitor connection"""
        # Check if connection name already exists
        for conn in self.connections:
            if conn['name'] == name:
                return False, "Connection name already exists"

        connection = {
            'name': name,
            'host': host,
            'username': username,
            'password': password  # Store password (optional)
        }

        self.connections.append(connection)
        if self.save_connections():
            return True, "Monitor connection saved successfully"
        return False, "Failed to save monitor connection"

    def update_connection(self, old_name, name, host, username, password=None):
        """Update an existing monitor connection"""
        for i, conn in enumerate(self.connections):
            if conn['name'] == old_name:
                self.connections[i] = {
                    'name': name,
                    'host': host,
                    'username': username,
                    'password': password  # Store password (optional)
                }
                if self.save_connections():
                    return True, "Monitor connection updated successfully"
                return False, "Failed to update monitor connection"
        return False, "Monitor connection not found"

    def delete_connection(self, name):
        """Delete a monitor connection"""
        for i, conn in enumerate(self.connections):
            if conn['name'] == name:
                self.connections.pop(i)
                if self.save_connections():
                    return True, "Monitor connection deleted successfully"
                return False, "Failed to delete monitor connection"
        return False, "Monitor connection not found"

    def get_connection(self, name):
        """Get a monitor connection by name"""
        for conn in self.connections:
            if conn['name'] == name:
                return conn
        return None

    def get_all_connections(self):
        """Get all saved monitor connections"""
        return self.connections

    def connection_exists(self, name):
        """Check if a monitor connection exists"""
        return any(conn['name'] == name for conn in self.connections)
