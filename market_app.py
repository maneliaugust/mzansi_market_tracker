import psycopg2
from db_setup import create_tables

def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="mzansi_market",
        user="postgres",
        password="mypassword"
    )
    return conn

def add_stall_owner(name, location):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        insert_query = "INSERT INTO Stall_Owners (name, location) VALUES (%s, %s) RETURNING id;"
        cursor.execute(insert_query, (name, location))
        owner_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Stall owner '{name}' successfully added with ID {owner_id}.")
        return owner_id
    except Exception as e:
        print(f"Error adding stall owner: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def add_product(owner_id, name, price, stock):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        insert_query = "INSERT INTO Products (owner_id, name, price, stock) VALUES (%s, %s, %s, %s) RETURNING id;"
        cursor.execute(insert_query, (owner_id, name, price, stock))
        product_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Product '{name}' successfully added with ID {product_id}.")
        return product_id
    except Exception as e:
        print(f"Error adding product: {e}")
        conn.rollback()
   


def make_sale(product_name, quantity):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Reduce stock first
        update_stock_query = "UPDATE Products SET stock = stock - %s WHERE name = %s;"
        cursor.execute(update_stock_query, (quantity, product_name))
        
        # Insert sale
        insert_query = "INSERT INTO Sales (product_name, quantity) VALUES (%s, %s) RETURNING id;"
        cursor.execute(insert_query, (product_name, quantity))
        sale_id = cursor.fetchone()[0]
        
        conn.commit()
        print(f"Sale made successfully! Sale ID: {sale_id}")
        return sale_id
    
    except Exception as e:
        print(f"Error making sale: {e}")
        

def weekly_report():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        report_query = """
        SELECT p.name, SUM(s.quantity) AS total_sold, SUM(s.total_amount) AS total_revenue
        FROM Sales s
        JOIN Products p ON s.product_id = p.id
        WHERE s.sale_date >= NOW() - INTERVAL '7 days'
        GROUP BY p.name;
        """
        cursor.execute(report_query)
        report = cursor.fetchall()
        print("Weekly report generated successfully!")
        return report
    except Exception as e:
        print(f"Error generating weekly report: {e}")
        
        