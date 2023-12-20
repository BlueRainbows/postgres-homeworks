"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv

path_1 = os.path.join('north_data/employees_data.csv')
path_2 = os.path.join('north_data/customers_data.csv')
path_3 = os.path.join('north_data/orders_data.csv')

with psycopg2.connect(
        host="localhost",
        database="north",
        user="postgres",
        password="Ad.Dam"
) as conn:
    with conn.cursor() as cur:
        with open(path_1, 'r', newline='') as file_emp:
            loads_employees = csv.DictReader(file_emp)
            for emp in loads_employees:
                cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                            (int(emp["employee_id"]),
                             emp["first_name"],
                             emp["last_name"],
                             emp["title"],
                             emp["birth_date"],
                             emp["notes"]))
        with open(path_2, 'r', newline='') as file_cust:
            loads_customers = csv.DictReader(file_cust)
            for cust in loads_customers:
                cur.execute('INSERT INTO customers VALUES (%s, %s, %s)',
                            (cust["customer_id"],
                             cust["company_name"],
                             cust["contact_name"]))
        with open(path_3, 'r', newline='') as file_ord:
            loads_orders = csv.DictReader(file_ord)
            for ord in loads_orders:
                cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                            (int(ord["order_id"]),
                             ord["customer_id"],
                             ord["employee_id"],
                             ord["order_date"],
                             ord["ship_city"]))
conn.close()
