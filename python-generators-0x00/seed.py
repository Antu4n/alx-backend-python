"""
Script to set up a MySQL database with a users table
and populate it with data from a CSV file using generators.
"""

import mysql.connector
import csv
import requests
import uuid
import io


def connect_db():
    """Connects to the MySQL database server.
    
    Returns:
        mysql.connector.connection.MySQLConnection: Database connection object
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root" 
        )
        print("Successfully connected to MySQL server")
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL server: {err}")
        raise


def create_database(connection):
    """Creates the ALX_prodev database if it doesn't exist.
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("Database ALX_prodev created or already exists")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Error creating database: {err}")
        raise


def connect_to_prodev():
    """Connects to the ALX_prodev database in MySQL.
    
    Returns:
        mysql.connector.connection.MySQLConnection: Database connection object
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root", 
            database="ALX_prodev"
        )
        print("Successfully connected to ALX_prodev database")
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to ALX_prodev database: {err}")
        raise


def create_table(connection):
    """Creates a table user_data if it doesn't exist with the required fields.
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id CHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age DECIMAL(5,2) NOT NULL,
                INDEX (user_id)
            )
        """)
        print("Table user_data created or already exists")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")
        raise


def fetch_data_generator(url):
    """Generator function that fetches data from CSV and yields one row at a time.
    
    Args:
        url: URL of the CSV file
        
    Yields:
        dict: A dictionary containing data for one row
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data
        csv_data = io.StringIO(response.text)
        csv_reader = csv.DictReader(csv_data)
        
        # Yield rows one by one
        for row in csv_reader:
            # Generate UUID for user_id
            row['user_id'] = str(uuid.uuid4())
            yield row
            
    except requests.exceptions.RequestException as err:
        print(f"Error fetching data: {err}")
        raise


def insert_data(connection, data):
    """Inserts data in the database if it doesn't exist.
    
    Args:
        connection: MySQL connection object
        data: Generator yielding dictionaries of user data
    """
    try:
        cursor = connection.cursor()
        
        # Use a generator to stream rows from the CSV
        inserted_count = 0
        for row in data:
            # Check if a user with the same email already exists
            cursor.execute(
                "SELECT COUNT(*) FROM user_data WHERE email = %s",
                (row['email'],)
            )
            count = cursor.fetchone()[0]
            
            if count == 0:
                cursor.execute(
                    """
                    INSERT INTO user_data (user_id, name, email, age)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        row['user_id'],
                        row['name'],
                        row['email'],
                        float(row['age'])
                    )
                )
                inserted_count += 1
                
                # Commit every 100 records to avoid large transactions
                if inserted_count % 100 == 0:
                    connection.commit()
                    print(f"Inserted {inserted_count} records so far...")
        
        # Final commit for any remaining records
        connection.commit()
        print(f"Successfully inserted {inserted_count} records")
        cursor.close()
        
    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")
        connection.rollback()
        raise


def main():
    """Main function to orchestrate the database setup and data import."""
    # URL of the CSV file
    csv_url = "https://s3.amazonaws.com/alx-intranet.hbtn.io/uploads/misc/2024/12/3888260f107e3701e3cd81af49ef997cf70b6395.csv"
    
    # Set up database and table
    conn = connect_db()
    create_database(conn)
    conn.close()
    
    # Connect to the ALX_prodev database and create tables
    conn = connect_to_prodev()
    create_table(conn)
    
    # Get data generator
    data_generator = fetch_data_generator(csv_url)
    
    # Insert data
    insert_data(conn, data_generator)
    
    # Close the connection
    conn.close()
    print("Database setup and data import completed successfully")


if __name__ == "__main__":
    main()
