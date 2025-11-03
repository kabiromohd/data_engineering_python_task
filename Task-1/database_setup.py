import duckdb

# Create a connection to duckdb
conn = duckdb.connect('/home/kabiromohd/data_engineering_python_task/Task-1/quote_task.db')

# create github event table
conn.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    subscription_status VARCHAR(10) CHECK (subscription_status IN ('active', 'inactive')) DEFAULT 'active',
    email_frequency VARCHAR(10) CHECK (email_frequency IN ('daily', 'weekly')) DEFAULT 'daily',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
""")

# Insert sample data into users table
conn.execute("""
INSERT INTO users 
(user_id, name, email, subscription_status, email_frequency)
VALUES
(1, 'Kabir Mohammed', 'kabirolawlemohammed@yahoo.com', 'active', 'daily'),
(2, 'Bala Usman', 'skyfortcafe@gmail.com', 'active', 'weekly'),
(3, 'Sanusi Badaru', 'skyfortcafe@yahoo.com', 'inactive', 'daily'),
(4, 'Abubakar Mohammed', 'skyfortglobalresourcesltd@yahoo.com', 'active', 'daily');
""")