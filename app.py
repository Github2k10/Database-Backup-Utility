import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from database.mysql.mysql_connection import MySQL as mysql


def create_mysql_backup():
    def create_connection():
        hostname = input("Enter MySQL hostname: ")
        port = input("Enter MySQL port: ")
        username = input("Enter MySQL username: ")
        password = input("Enter MySQL password: ")
        
        return mysql(host=hostname, port=port, username=username, password=password)
        
    mysql_connection = create_connection()
    
    if mysql_connection.is_connected():
        while True:
            print("1. Check database connection \n2. Create DB backup \n3. Create table backup \n4. Close connection \n5. Create new connection \n0. Exit")
            choice = input("Enter your choice: ")
            
            if choice == "1":
                if mysql_connection.is_connected():
                    print("Database connection is active.")
                else:
                    print("Database connection is not active.")
            elif choice == "2":
                print("Enter database name: ")
                database_name = input()
                
                print("Enter backup file name: ")
                backup_file = input()
                
                if not mysql_connection.is_connected():
                    print("Database connection is not active. Please try again.")
                    continue
                
                mysql_connection.database_backup(database=database_name, backup_file=backup_file)
            elif choice == "3":
                print("Enter database name: ")
                database_name = input()
                
                print("Enter table name separated by space: ")
                table_name = input()
                table_name = [x.strip() if len(x) > 0 else x for x in table_name.split(" ")]
                
                print("Enter backup file name: ")
                backup_file = input()
                
                if not mysql_connection.is_connected():
                    print("Database connection is not active. Please try again.")
                    continue
                
                mysql_connection.database_backup(database=database_name, backup_file=backup_file, tables=table_name)
                
            elif choice == "4":
                if mysql_connection.close_connection():
                    print("Connection closed successfully.")
                else:
                    print("Unable to close the connection.")
            elif choice == "5":
                if mysql_connection.is_connected():
                    mysql_connection.close_connection()
                    print("Previous connection closed.")
                    
                mysql_connection = create_connection()
            elif choice == "0":
                return
            else:
                print("Invalid choice. Please try again.")
    else:
        while True:
            print("Invalid credintial or enable to connect with DB. Try again...\n")
            print("1. Try again \n0. Exit")
            choice = input("Enter you choise: ")
            
            if choice == "1":
                create_mysql_backup()
            elif choice == "0":
                return
            else: 
                print("Invalid choice. Please try again.")

def new_backup():
    while True:
        print("1. MySQL \n2. PostgreSQL \n3. MongoDB \n0. Exit")
        choice = input("Enter your choice: ")
        
        if choice == "1":
            create_mysql_backup()
        elif choice == "2":
            pass
        elif choice == "3":
            pass
        elif choice == '0':
            break
        else:
            print("Invalid choice. Please try again.")

def restore_db():
    pass

def main():
    while True:
        print("1. New Backup \n2. Restore DB \n0. Exit")
        choice = input("Enter your choice: ")
        if choice == "1":
            new_backup()
        elif choice == "2":
            restore_db()
        elif choice == "0":
            print("Thank you for vising.")
            exit(1)
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()