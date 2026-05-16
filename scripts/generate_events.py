import random
import time
from decimal import Decimal

import mysql.connector


STATUSES = [
    "PENDING",
    "PAID",
    "SHIPPED",
    "CANCELLED",
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


conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="cdc_user",
    password="cdc_password",
    database="cdc_demo",
)

cursor = conn.cursor()


def insert_order():
    customer = random.choice(CUSTOMER_NAMES)

    amount = round(random.uniform(100, 5000), 2)

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

    print(f"[INSERT] customer={customer} amount={amount}")


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

    print(f"[UPDATE] id={order_id} status={new_status}")


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

    cursor.execute(query, (order_id,))

    conn.commit()

    print(f"[DELETE] id={order_id}")


OPERATIONS = [
    insert_order,
    update_order,
    delete_order,
]


try:

    while True:

        operation = random.choice(OPERATIONS)

        operation()

        sleep_time = random.uniform(0.2, 1.5)

        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\nStopping generator...")

finally:
    cursor.close()
    conn.close()