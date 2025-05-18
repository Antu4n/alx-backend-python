#!/usr/bin/python3
"""
Generators to fetch and process users in batches from the database
"""

import mysql.connector


def stream_users_in_batches(batch_size):
    """
    Generator that fetches users from the database in batches of specified size.
    
    Args:
        batch_size (int): Number of users to fetch in each batch
        
    Yields:
        list: A batch of users as a list of dictionaries
    """
    try:
        # Establish database connection
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",  # Empty password - modify if needed
            database="ALX_prodev"
        )
        
        cursor = connection.cursor(dictionary=True)
        
        # Initialize offset for pagination
        offset = 0
        
        # Single loop to fetch batches
        while True:
            # Execute query with LIMIT and OFFSET for batch fetching
            query = f"SELECT user_id, name, email, age FROM user_data LIMIT {batch_size} OFFSET {offset}"
            cursor.execute(query)
            
            # Fetch the batch
            batch = cursor.fetchall()
            
            # If batch is empty, we've reached the end
            if not batch:
                break
                
            # Yield the batch
            yield batch
            
            # Update offset for next batch
            offset += batch_size
        
        # Clean up resources
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        try:
            # Try alternative connection method if first one fails
            connection = mysql.connector.connect(
                host="localhost",
                user="root",  # No password
                database="ALX_prodev"
            )
            
            cursor = connection.cursor(dictionary=True)
            offset = 0
            
            while True:
                query = f"SELECT user_id, name, email, age FROM user_data LIMIT {batch_size} OFFSET {offset}"
                cursor.execute(query)
                
                batch = cursor.fetchall()
                if not batch:
                    break
                    
                yield batch
                offset += batch_size
                
            cursor.close()
            connection.close()
            
        except mysql.connector.Error as err2:
            print(f"Failed to connect to database: {err2}")
            return


def batch_processing(batch_size):
    """
    Process batches of users and filter those over the age of 25.
    
    Args:
        batch_size (int): Size of batches to process
    """
    # Get the batch generator
    batch_generator = stream_users_in_batches(batch_size)
    
    # Loop through batches
    for batch in batch_generator:
        # Process users in the batch and yield filtered users
        for user in batch:
            # Filter users over age 25
            if float(user['age']) > 25:
                # Print the filtered user with nice formatting
                print(user)
                print()  # Empty line between users
