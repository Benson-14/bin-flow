import random
import time
from decimal import Decimal

import mysql.connector


# ==========================================
# Fake Data
# ==========================================

STATUSES = [
    "PENDING",
    "PAID",
    "SHIPPED",
    "CANCELLED",
]

PAYMENT_STATUSES = [
    "INITIATED",
    "SUCCESS",
    "FAILED",
    "REFUNDED",
]

PAYMENT_METHODS = [
    "CARD",
    "UPI",
    "NETBANKING",
    "WALLET",
]

CUSTOMER_NAMES = [
    "Benson",
    "Alex",
    "Pedro",
    "Maria",
    "John",
    "Sara",
    "David",
]

CITIES = [
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Chennai",
]


# ==========================================
# MySQL Connection
# ==========================================

conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="cdc_user",
    password="cdc_password",
    database="cdc_demo",
)

cursor = conn.cursor()


# ==========================================
# Create Tables
# ==========================================

def create_tables():

    orders_table = """
    CREATE TABLE IF NOT EXISTS orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_name VARCHAR(255),
        amount DECIMAL(10,2),
        status VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    customers_table = """
    CREATE TABLE IF NOT EXISTS customers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        full_name VARCHAR(255),
        email VARCHAR(255),
        city VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    payments_table = """
    CREATE TABLE IF NOT EXISTS payments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT,
        amount DECIMAL(10,2),
        payment_method VARCHAR(50),
        payment_status VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    cursor.execute(orders_table)
    cursor.execute(customers_table)
    cursor.execute(payments_table)

    conn.commit()

    print("[SETUP] tables created successfully")


# ==========================================
# Orders Operations
# ==========================================

def insert_order():

    customer = random.choice(CUSTOMER_NAMES)

    amount = round(
        random.uniform(100, 5000),
        2,
    )

    status = random.choice(STATUSES)

    query = """
    INSERT INTO orders (
        customer_name,
        amount,
        status
    )
    VALUES (%s, %s, %s)
    """

    cursor.execute(
        query,
        (
            customer,
            Decimal(str(amount)),
            status,
        ),
    )

    conn.commit()

    print(
        f"[ORDER INSERT] "
        f"customer={customer} "
        f"amount={amount}"
    )


def update_order():

    cursor.execute(
        "SELECT id FROM orders ORDER BY RAND() LIMIT 1"
    )

    row = cursor.fetchone()

    if not row:
        return

    order_id = row[0]

    new_status = random.choice(STATUSES)

    query = """
    UPDATE orders
    SET status = %s
    WHERE id = %s
    """

    cursor.execute(
        query,
        (
            new_status,
            order_id,
        ),
    )

    conn.commit()

    print(
        f"[ORDER UPDATE] "
        f"id={order_id} "
        f"status={new_status}"
    )


def delete_order():

    cursor.execute(
        "SELECT id FROM orders ORDER BY RAND() LIMIT 1"
    )

    row = cursor.fetchone()

    if not row:
        return

    order_id = row[0]

    query = """
    DELETE FROM orders
    WHERE id = %s
    """

    cursor.execute(
        query,
        (order_id,),
    )

    conn.commit()

    print(
        f"[ORDER DELETE] id={order_id}"
    )


# ==========================================
# Customers Operations
# ==========================================

def insert_customer():

    name = random.choice(CUSTOMER_NAMES)

    email = (
        name.lower()
        + str(random.randint(1, 999))
        + "@gmail.com"
    )

    city = random.choice(CITIES)

    query = """
    INSERT INTO customers (
        full_name,
        email,
        city
    )
    VALUES (%s, %s, %s)
    """

    cursor.execute(
        query,
        (
            name,
            email,
            city,
        ),
    )

    conn.commit()

    print(
        f"[CUSTOMER INSERT] "
        f"name={name} "
        f"city={city}"
    )


def update_customer():

    cursor.execute(
        "SELECT id FROM customers ORDER BY RAND() LIMIT 1"
    )

    row = cursor.fetchone()

    if not row:
        return

    customer_id = row[0]

    new_city = random.choice(CITIES)

    query = """
    UPDATE customers
    SET city = %s
    WHERE id = %s
    """

    cursor.execute(
        query,
        (
            new_city,
            customer_id,
        ),
    )

    conn.commit()

    print(
        f"[CUSTOMER UPDATE] "
        f"id={customer_id} "
        f"city={new_city}"
    )


# ==========================================
# Payments Operations
# ==========================================

def insert_payment():

    cursor.execute(
        "SELECT id FROM orders ORDER BY RAND() LIMIT 1"
    )

    row = cursor.fetchone()

    if not row:
        return

    order_id = row[0]

    amount = round(
        random.uniform(50, 3000),
        2,
    )

    payment_method = random.choice(
        PAYMENT_METHODS
    )

    payment_status = random.choice(
        PAYMENT_STATUSES
    )

    query = """
    INSERT INTO payments (
        order_id,
        amount,
        payment_method,
        payment_status
    )
    VALUES (%s, %s, %s, %s)
    """

    cursor.execute(
        query,
        (
            order_id,
            Decimal(str(amount)),
            payment_method,
            payment_status,
        ),
    )

    conn.commit()

    print(
        f"[PAYMENT INSERT] "
        f"order_id={order_id} "
        f"amount={amount}"
    )


def update_payment():

    cursor.execute(
        "SELECT id FROM payments ORDER BY RAND() LIMIT 1"
    )

    row = cursor.fetchone()

    if not row:
        return

    payment_id = row[0]

    new_status = random.choice(
        PAYMENT_STATUSES
    )

    query = """
    UPDATE payments
    SET payment_status = %s
    WHERE id = %s
    """

    cursor.execute(
        query,
        (
            new_status,
            payment_id,
        ),
    )

    conn.commit()

    print(
        f"[PAYMENT UPDATE] "
        f"id={payment_id} "
        f"status={new_status}"
    )


# ==========================================
# Operations Pool
# ==========================================

OPERATIONS = [
    insert_order,
    update_order,
    delete_order,
    insert_customer,
    update_customer,
    insert_payment,
    update_payment,
]


# ==========================================
# Main
# ==========================================

create_tables()

try:

    while True:

        operation = random.choice(
            OPERATIONS
        )

        operation()

        sleep_time = random.uniform(
            0.2,
            1.5,
        )

        time.sleep(sleep_time)

except KeyboardInterrupt:

    print("\nStopping generator...")

finally:

    cursor.close()
    conn.close()