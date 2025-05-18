#!/usr/bin/python3
"""
Generator function to stream users from the user_data table one by one
"""

import mysql.connector


def stream_users():
    """
    A generator function that fetches rows one by one from the user_data table
    and yields them as dictionaries.

    Yields:
        dict: Dictionary containing user data with keys 'user_id', 'name', 'email', and 'age'
    """
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",  
            database="ALX_prodev"
        )
        
        # Create a cursor and execute the query
        cursor = connection.cursor(dictionary=True)  # Returns rows as dictionaries
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        
        # Use a single loop to fetch and yield rows one by one
        for row in cursor:
            yield row
            
        # Clean up resources
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        # Try alternative connection if the first one fails
        try:
            connection = mysql.connector.connect(
                host="localhost",
                user="root",  # Try without password parameter
                database="ALX_prodev"
            )
            
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT user_id, name, email, age FROM user_data")
            
            for row in cursor:
                yield row
                
            cursor.close()
            connection.close()
            
        except mysql.connector.Error as err2:
            print(f"Failed to connect to database: {err2}")
            # Yield empty result or raise exception
            return
