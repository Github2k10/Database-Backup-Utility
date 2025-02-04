import mysql.connector
from mysql.connector import Error
import subprocess
import os

class MySQL:
    def __init__(self, host, username, password, port=None):
        self.hostname = host
        self.username = username
        self.password = password
        self.port = port
        self.connection = self.new_connection()
        
        if self.is_connected():
            self.cursor = self.connection.cursor() 
        else:
            self.cursor = None
            
        self.backup_dir = "backup/mysql/"
        
    def is_connected(self):
        try:
            if self.connection and self.connection.is_connected():
                return True
            else:
                return False
        except Error as e:
            print(f"Error: {e}")
            return False
        
    def new_connection(self):
        try:
            if self.port:
                self.connection = mysql.connector.connect(
                    host=self.hostname,
                    user=self.username,
                    password=self.password,
                    port=self.port
                )
            else:
                self.connection = mysql.connector.connect(
                    host=self.hostname,
                    user=self.username,
                    password=self.password
                )
                
            if self.connection.is_connected():
                print(f"Connected to MySQL Server {self.hostname}")
                self.cursor = self.connection.cursor() 
                
                return self.connection
            else:
                print("Failed to connect")
                return None
        except Error as e:
            print(f"Error: {e}")
            return None
        
    def close_connection(self):
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                return True
            else:
                return False
        except Error as e:
            print(f"Error: {e}")
            return False
        
    def database_backup(self, database, backup_file, tables=[]):
        try:
            if not all([database, backup_file]):
                print("Error: One or more input parameters are None.")
                return
            
            # Prepare the command for mysqldump
            command = [
                'mysqldump',
                '-h', self.hostname,
                '-u', self.username,
                '--password=' + self.password,
                database
            ]
            
            if not self.check_DB(database=database):
                print(f"Database {database} does not exist.")
                return
            
            if tables:    
                table_list = []
                table_not_exist = []
                new_tables = self.get_Tables(database=database)
                
                for table in tables:
                    if table in new_tables:
                        table_list.append(table)
                    else:
                        table_not_exist.append(table)
                        
                if len(table_not_exist) > 0:
                    print(f"Table(s) {', '.join(table_not_exist)} do not exist.")
                    return
            
                command.extend(table_list)
                
            if not os.path.exists(self.backup_dir):
                os.makedirs(self.backup_dir)
            
            # Ensure the backup file has a .sql extension
            backup_file = backup_file if backup_file.endswith(".sql") else backup_file + ".sql"
            backup_file = self.backup_dir + backup_file  

            with open(backup_file, 'wb') as f:
                # Run the mysqldump command and write the output to the file
                subprocess.run(command, stdout=f, stderr=subprocess.PIPE, check=True)
                print(f"Backup of database '{database}' saved to {backup_file}")
        
        except Error as e:
            print(f"Error: {e}")
            return False
    
    def get_cursor(self):
        return self.cursor

    def check_DB(self, database):
        try:
            cursor = self.connection.cursor()
            cursor.execute("show databases;")
            result = cursor.fetchall()
            database_names = [db[0] for db in result]
            
            return database in database_names
        except Error as e:
            print(f"Error: {e}")
            return False
        
    def get_Tables(self, database):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"use {database};")
            cursor.execute("show tables;")
            result = cursor.fetchall()
            table_names = [db[0] for db in result]
            
            return table_names
        except Error as e:
            print(f"Error: {e}")
            return False