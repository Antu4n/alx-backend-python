def paginate_users(page_size, offset):
    import seed
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM user_data LIMIT {page_size} OFFSET {offset}")
    rows = cursor.fetchall()
    connection.close()
    return rows

def stream_user_ages(page_size=100):
    offset = 0
    while True:
        users = paginate_users(page_size, offset)
        if not users:
            break
        for user in users:
            yield user['age']
        offset += page_size

def compute_average_age():
    total_age = 0
    count = 0
    for age in stream_user_ages():
        total_age += age
        count += 1
    average = total_age / count if count > 0 else 0
    print(f"Average age of users: {average:.2f}")

# Only call if this is the main script
if __name__ == "__main__":
    compute_average_age()
