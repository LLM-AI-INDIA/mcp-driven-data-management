import os
import pyodbc
import psycopg2
from typing import Any

# MCP server
from fastmcp import FastMCP
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


def must_get(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var {key}")
    return val


# ————————————————
# 1. MySQL Configuration
# ————————————————
MYSQL_HOST = must_get("MYSQL_HOST")
MYSQL_PORT = int(must_get("MYSQL_PORT"))
MYSQL_USER = must_get("MYSQL_USER")
MYSQL_PASSWORD = must_get("MYSQL_PASSWORD")
MYSQL_DB = must_get("MYSQL_DB")


def get_mysql_conn(db: str | None = MYSQL_DB):
    """If db is None we connect to the server only (needed to CREATE DATABASE)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        ssl_disabled=False,  # Aiven requires TLS; keep this False
        autocommit=True,
    )


# ————————————————
# 2. PostgreSQL Configuration (Products)
# ————————————————
PG_HOST = must_get("PG_HOST")
PG_PORT = int(must_get("PG_PORT"))
PG_DB = os.getenv("PG_DB", "postgres")  # db name can default
PG_USER = must_get("PG_USER")
PG_PASS = must_get("PG_PASSWORD")


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",  # Supabase enforces TLS
    )


# ————————————————
# 3. PostgreSQL Configuration (Sales)
# ————————————————
PG_SALES_HOST = must_get("PG_SALES_HOST")
PG_SALES_PORT = int(must_get("PG_SALES_PORT"))
PG_SALES_DB = os.getenv("PG_SALES_DB", "sales_db")
PG_SALES_USER = must_get("PG_SALES_USER")
PG_SALES_PASS = must_get("PG_SALES_PASSWORD")


def get_pg_sales_conn():
    return psycopg2.connect(
        host=PG_SALES_HOST,
        port=PG_SALES_PORT,
        dbname=PG_SALES_DB,
        user=PG_SALES_USER,
        password=PG_SALES_PASS,
        sslmode="require",
    )


# ————————————————
# 4. Instantiate your MCP server
# ————————————————
mcp = FastMCP("CRUDServer")


# ————————————————
# 5. Synchronous Setup: Create & seed tables
# ————————————————
def seed_databases():
    # ---------- MySQL (Customers) ----------
    root_cnx = get_mysql_conn(db=None)
    root_cur = root_cnx.cursor()
    root_cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}`;")
    root_cur.close()
    root_cnx.close()

    sql_cnx = get_mysql_conn()
    sql_cur = sql_cnx.cursor()

    # Disable foreign key checks temporarily
    sql_cur.execute("SET FOREIGN_KEY_CHECKS = 0;")

    # Drop tables in reverse dependency order (Sales first, then referenced tables)
    sql_cur.execute("DROP TABLE IF EXISTS Sales;")
    sql_cur.execute("DROP TABLE IF EXISTS ProductsCache;")
    sql_cur.execute("DROP TABLE IF EXISTS Customers;")

    # Re-enable foreign key checks
    sql_cur.execute("SET FOREIGN_KEY_CHECKS = 1;")

    # Create Customers table with FirstName and LastName
    sql_cur.execute("""
                    CREATE TABLE Customers
                    (
                        Id        INT AUTO_INCREMENT PRIMARY KEY,
                        FirstName VARCHAR(50) NOT NULL,
                        LastName  VARCHAR(50) NOT NULL,
                        Name      VARCHAR(100) NOT NULL,
                        Email     VARCHAR(100),
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)

    # Insert sample customers with FirstName and LastName
    sql_cur.executemany(
        "INSERT INTO Customers (FirstName, LastName, Name, Email) VALUES (%s, %s, %s, %s)",
        [("Alice", "Johnson", "Alice Johnson", "alice@example.com"),
         ("Bob", "Smith", "Bob Smith", "bob@example.com"),
         ("Charlie", "Brown", "Charlie Brown", None)]  # Charlie has no email for null handling demo
    )

    # Create ProductsCache table (copy of PostgreSQL products for easier joins)
    sql_cur.execute("""
                    CREATE TABLE ProductsCache
                    (
                        id          INT PRIMARY KEY,
                        name        VARCHAR(100) NOT NULL,
                        price       DECIMAL(10, 4) NOT NULL,
                        description TEXT
                    );
                    """)

    # Insert sample products cache
    sql_cur.executemany(
        "INSERT INTO ProductsCache (id, name, price, description) VALUES (%s, %s, %s, %s)",
        [(1, "Widget", 9.99, "A standard widget."),
         (2, "Gadget", 14.99, "A useful gadget."),
         (3, "Tool", 24.99, None)]  # Tool has no description for null handling demo
    )

    # Create Sales table in MySQL with foreign key constraints
    sql_cur.execute("""
                    CREATE TABLE Sales
                    (
                        Id           INT AUTO_INCREMENT PRIMARY KEY,
                        customer_id  INT            NOT NULL,
                        product_id   INT            NOT NULL,
                        quantity     INT            NOT NULL DEFAULT 1,
                        unit_price   DECIMAL(10, 4) NOT NULL,
                        total_price  DECIMAL(10, 4) NOT NULL,
                        sale_date    TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (customer_id) REFERENCES Customers(Id) ON DELETE CASCADE,
                        FOREIGN KEY (product_id) REFERENCES ProductsCache(id) ON DELETE CASCADE
                    );
                    """)

    # Insert sample sales data
    sql_cur.executemany(
        "INSERT INTO Sales (customer_id, product_id, quantity, unit_price, total_price) VALUES (%s, %s, %s, %s, %s)",
        [(1, 1, 2, 9.99, 19.98),  # Alice bought 2 Widgets
         (2, 2, 1, 14.99, 14.99),  # Bob bought 1 Gadget
         (3, 3, 3, 24.99, 74.97)]  # Charlie bought 3 Tools
    )

    sql_cnx.close()

    # ---------- PostgreSQL (Products) ----------
    pg_cnxn = get_pg_conn()
    pg_cnxn.autocommit = True
    pg_cur = pg_cnxn.cursor()

    pg_cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    pg_cur.execute("""
                   CREATE TABLE products
                   (
                       id          SERIAL PRIMARY KEY,
                       name        TEXT           NOT NULL,
                       price       NUMERIC(10, 4) NOT NULL,
                       description TEXT
                   );
                   """)

    pg_cur.executemany(
        "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)",
        [("Widget", 9.99, "A standard widget."),
         ("Gadget", 14.99, "A useful gadget."),
         ("Tool", 24.99, "A handy tool.")]
    )
    pg_cnxn.close()

    # ---------- PostgreSQL Sales Database ----------
    sales_cnxn = get_pg_sales_conn()
    sales_cnxn.autocommit = True
    sales_cur = sales_cnxn.cursor()

    sales_cur.execute("DROP TABLE IF EXISTS sales;")
    sales_cur.execute("""
                      CREATE TABLE sales
                      (
                          id           SERIAL PRIMARY KEY,
                          customer_id  INT            NOT NULL,
                          product_id   INT            NOT NULL,
                          quantity     INT            NOT NULL DEFAULT 1,
                          unit_price   NUMERIC(10, 4) NOT NULL,
                          total_amount NUMERIC(10, 4) NOT NULL,
                          sale_date    TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
                      );
                      """)

    # Sample sales data
    sales_cur.executemany(
        "INSERT INTO sales (customer_id, product_id, quantity, unit_price, total_amount) VALUES (%s, %s, %s, %s, %s)",
        [(1, 1, 2, 9.99, 19.98),  # Alice bought 2 Widgets
         (2, 2, 1, 14.99, 14.99),  # Bob bought 1 Gadget
         (3, 3, 3, 24.99, 74.97)]  # Charlie bought 3 Tools
    )
    sales_cnxn.close()


# ————————————————
# 6. Helper Functions for Cross-Database Queries and Name Resolution
# ————————————————
def get_customer_name(customer_id: int) -> str:
    """Fetch customer name from MySQL database"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        mysql_cur.execute("SELECT Name FROM Customers WHERE Id = %s", (customer_id,))
        result = mysql_cur.fetchone()
        mysql_cnxn.close()
        return result[0] if result else f"Unknown Customer ({customer_id})"
    except Exception:
        return f"Unknown Customer ({customer_id})"


def get_product_details(product_id: int) -> dict:
    """Fetch product name and price from PostgreSQL products database"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        pg_cur.execute("SELECT name, price FROM products WHERE id = %s", (product_id,))
        result = pg_cur.fetchone()
        pg_cnxn.close()
        if result:
            return {"name": result[0], "price": float(result[1])}
        else:
            return {"name": f"Unknown Product ({product_id})", "price": 0.0}
    except Exception:
        return {"name": f"Unknown Product ({product_id})", "price": 0.0}


def validate_customer_exists(customer_id: int) -> bool:
    """Check if customer exists in MySQL database"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        mysql_cur.execute("SELECT COUNT(*) FROM Customers WHERE Id = %s", (customer_id,))
        result = mysql_cur.fetchone()
        mysql_cnxn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def validate_product_exists(product_id: int) -> bool:
    """Check if product exists in PostgreSQL products database"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        pg_cur.execute("SELECT COUNT(*) FROM products WHERE id = %s", (product_id,))
        result = pg_cur.fetchone()
        pg_cnxn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def find_customer_by_name(name: str) -> dict:
    """Find customer by name (supports partial matching)"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()

        # Try exact match first
        mysql_cur.execute("SELECT Id, Name FROM Customers WHERE Name = %s", (name,))
        result = mysql_cur.fetchone()

        if result:
            mysql_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}

        # Try case-insensitive exact match
        mysql_cur.execute("SELECT Id, Name FROM Customers WHERE LOWER(Name) = LOWER(%s)", (name,))
        result = mysql_cur.fetchone()

        if result:
            mysql_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}

        # Try partial match on first name, last name, or full name
        mysql_cur.execute("""
            SELECT Id, Name FROM Customers
            WHERE LOWER(FirstName) = LOWER(%s)
               OR LOWER(LastName) = LOWER(%s)
               OR LOWER(Name) LIKE LOWER(%s)
        """, (name, name, f"%{name}%"))
        result = mysql_cur.fetchone()

        if result:
            mysql_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}

        mysql_cnxn.close()
        return {"found": False, "error": f"Customer '{name}' not found"}

    except Exception as e:
        return {"found": False, "error": f"Database error: {str(e)}"}


def find_product_by_name(name: str) -> dict:
    """Find product by name (supports partial matching)"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()

        # Try exact match first
        pg_cur.execute("SELECT id, name FROM products WHERE name = %s", (name,))
        result = pg_cur.fetchone()

        if result:
            pg_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}

        # Try case-insensitive exact match
        pg_cur.execute("SELECT id, name FROM products WHERE LOWER(name) = LOWER(%s)", (name,))
        result = pg_cur.fetchone()

        if result:
            pg_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}

        # Try partial match
        pg_cur.execute("SELECT id, name FROM products WHERE LOWER(name) LIKE LOWER(%s)", (f"%{name}%",))
        result = pg_cur.fetchone()

        if result:
            pg_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}

        pg_cnxn.close()
        return {"found": False, "error": f"Product '{name}' not found"}

    except Exception as e:
        return {"found": False, "error": f"Database error: {str(e)}"}


def execute_sales_operation_with_where(operation: str, where_condition: str, **kwargs):
    """Execute sales operations with WHERE clause support"""
    sales_cnxn = get_pg_sales_conn()
    sales_cur = sales_cnxn.cursor()

    try:
        if operation == "delete" and where_condition:
            sql_query = f"DELETE FROM sales WHERE {where_condition}"
            sales_cur.execute(sql_query)
            affected_rows = sales_cur.rowcount
            sales_cnxn.commit()
            result = f"✅ Deleted {affected_rows} sales records matching condition: {where_condition}"

        elif operation == "update" and where_condition:
            # Build UPDATE SET clause based on provided parameters
            set_clauses = []
            params = []

            if kwargs.get('new_quantity') is not None:
                set_clauses.append("quantity = %s")
                set_clauses.append("total_amount = unit_price * %s")
                params.extend([kwargs['new_quantity'], kwargs['new_quantity']])

            if kwargs.get('new_price') is not None:
                set_clauses.append("unit_price = %s")
                set_clauses.append("total_amount = %s * quantity")
                params.extend([kwargs['new_price'], kwargs['new_price']])

            if not set_clauses:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ No update parameters provided (new_quantity or new_price required)"}

            sql_query = f"UPDATE sales SET {', '.join(set_clauses)} WHERE {where_condition}"
            sales_cur.execute(sql_query, params)
            affected_rows = sales_cur.rowcount
            sales_cnxn.commit()
            result = f"✅ Updated {affected_rows} sales records matching condition: {where_condition}"

        else:
            sales_cnxn.close()
            return {"sql": None, "result": f"❌ WHERE clause not supported for {operation} or missing condition"}

        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    except Exception as e:
        sales_cnxn.close()
        return {"sql": None, "result": f"❌ Error executing {operation} with WHERE clause: {str(e)}"}


# ————————————————
# 7. Enhanced MySQL CRUD Tool (Customers) with Smart Name Resolution
# ————————————————
# You will need to define: get_mysql_conn(), get_pg_conn(), find_customer_by_name(), find_product_by_name(), etc.

@mcp.tool(name="CustomerManagement", description="Create, list, update, or delete customers in the MySQL customer database. Handles keywords like customer name, email, and ID.")
async def sqlserver_crud(
        operation: str,
        name: str = None,
        email: str = None,
        limit: int = 10,
        customer_id: int = None,
        new_email: str = None,
        new_last_name: str = None,
        customer_first_name: str = None,
        customer_last_name: str = None,
        customer_email: str = None,
        table_name: str = None,
) -> Any:
    cnxn = get_mysql_conn()
    cur = cnxn.cursor()

    # 🔁 Normalize alternate fields
    if customer_first_name or customer_last_name:
        first = customer_first_name or ""
        last = customer_last_name or ""
        name = f"{first} {last}".strip()
    if customer_email:
        email = customer_email
    if new_last_name:
        name = name or ""
        name_parts = name.split(' ', 1)
        name = f"{name_parts[0]} {new_last_name}"

    if operation == "create":
        if not name or not email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}

        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        sql_query = "INSERT INTO Customers (FirstName, LastName, Name, Email) VALUES (%s, %s, %s, %s)"
        cur.execute(sql_query, (first_name, last_name, name, email))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Customer '{name}' added."}

    elif operation == "read":
        if name:
            sql_query = """
                SELECT Id, FirstName, LastName, Name, Email, CreatedAt
                FROM Customers
                WHERE LOWER(Name) LIKE LOWER(%s)
                   OR LOWER(FirstName) LIKE LOWER(%s)
                   OR LOWER(LastName) LIKE LOWER(%s)
                ORDER BY Id ASC
                LIMIT %s
            """
            cur.execute(sql_query, (f"%{name}%", f"%{name}%", f"%{name}%", limit))
        else:
            sql_query = """
                SELECT Id, FirstName, LastName, Name, Email, CreatedAt
                FROM Customers
                ORDER BY Id ASC
                LIMIT %s
            """
            cur.execute(sql_query, (limit,))

        rows = cur.fetchall()
        result = [
            {
                "Id": r[0], "FirstName": r[1], "LastName": r[2],
                "Name": r[3], "Email": r[4], "CreatedAt": r[5].isoformat()
            } for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not customer_id and name:
            customer_info = find_customer_by_name(name)
            if not customer_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {customer_info['error']}"}
            customer_id = customer_info["id"]

        if not customer_id or not new_email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' (or 'name') and 'new_email' required."}

        sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
        cur.execute(sql_query, (new_email, customer_id))
        cnxn.commit()

        cur.execute("SELECT Name FROM Customers WHERE Id = %s", (customer_id,))
        customer_name = cur.fetchone()
        customer_name = customer_name[0] if customer_name else f"Customer {customer_id}"

        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Customer '{customer_name}' email updated to '{new_email}'."}

    elif operation == "delete":
        if not customer_id and name:
            customer_info = find_customer_by_name(name)
            if not customer_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {customer_info['error']}"}
            customer_id = customer_info["id"]
            customer_name = customer_info["name"]
        elif customer_id:
            cur.execute("SELECT Name FROM Customers WHERE Id = %s", (customer_id,))
            result = cur.fetchone()
            customer_name = result[0] if result else f"Customer {customer_id}"
        else:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' or 'name' required for delete."}

        sql_query = "DELETE FROM Customers WHERE Id = %s"
        cur.execute(sql_query, (customer_id,))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Customer '{customer_name}' deleted."}

    elif operation == "describe":
        table = table_name or "Customers"
        sql_query = f"DESCRIBE {table}"
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {
                "Field": r[0], "Type": r[1], "Null": r[2],
                "Key": r[3], "Default": r[4], "Extra": r[5]
            } for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 8. Enhanced PostgreSQL CRUD Tool (Products) with Smart Name Resolution
# ————————————————

@mcp.tool(name="ProductManagement", description="Manage products in the PostgreSQL products database: add, list, update, or delete")
async def postgresql_crud(
        operation: str,
        name: str = None,
        price: float = None,
        description: str = None,
        limit: int = 10,
        product_id: int = None,
        new_price: float = None,
        table_name: str = None
) -> Any:
    cnxn = get_pg_conn()
    cur = cnxn.cursor()

    if operation == "create":
        if not name or price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'price' required for create."}

        sql_query = "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)"
        cur.execute(sql_query, (name, price, description))
        cnxn.commit()
        result = f"✅ Product '{name}' added with price ${price:.2f}."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "read":
        if name:
            sql_query = """
                SELECT id, name, price, description
                FROM products
                WHERE LOWER(name) LIKE LOWER(%s)
                ORDER BY id ASC
                LIMIT %s
            """
            cur.execute(sql_query, (f"%{name}%", limit))
        else:
            sql_query = """
                SELECT id, name, price, description
                FROM products
                ORDER BY id ASC
                LIMIT %s
            """
            cur.execute(sql_query, (limit,))

        rows = cur.fetchall()
        result = [
            {"id": r[0], "name": r[1], "price": float(r[2]), "description": r[3] or ""}
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not product_id and name:
            product_info = find_product_by_name(name)
            if not product_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {product_info['error']}"}
            product_id = product_info["id"]

        if not product_id or new_price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' (or 'name') and 'new_price' required."}

        sql_query = "UPDATE products SET price = %s WHERE id = %s"
        cur.execute(sql_query, (new_price, product_id))
        cnxn.commit()

        cur.execute("SELECT name FROM products WHERE id = %s", (product_id,))
        product_name = cur.fetchone()
        product_name = product_name[0] if product_name else f"Product {product_id}"

        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Product '{product_name}' price updated to ${new_price:.2f}."}

    elif operation == "delete":
        if not product_id and name:
            product_info = find_product_by_name(name)
            if not product_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {product_info['error']}"}
            product_id = product_info["id"]
            product_name = product_info["name"]
        elif product_id:
            cur.execute("SELECT name FROM products WHERE id = %s", (product_id,))
            result = cur.fetchone()
            product_name = result[0] if result else f"Product {product_id}"
        else:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' or 'name' required for delete."}

        sql_query = "DELETE FROM products WHERE id = %s"
        cur.execute(sql_query, (product_id,))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Product '{product_name}' deleted."}

    elif operation == "describe":
        table = table_name or "products"
        sql_query = f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        cur.execute(sql_query, (table,))
        rows = cur.fetchall()
        result = [
            {
                "Column": r[0],
                "Type": r[1],
                "Nullable": r[2],
                "Default": r[3]
            } for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 9. Enhanced Sales CRUD Tool with Column Filtering and WHERE Clause Support
# ————————————————
@mcp.tool(name="SalesManagement", description="Manage sales records across customers and products: create, update, delete, query sales in PostgreSQL and MySQL")
async def sales_crud(
    operation: str,
    customer_id: int = None,
    product_id: int = None,
    quantity: int = 1,
    unit_price: float = None,
    total_amount: float = None,
    sale_id: int = None,
    new_quantity: int = None,
    table_name: str = None,
    display_format: str = None,
    customer_name: str = None,
    product_name: str = None,
    email: str = None,
    total_price: float = None,
    columns: str = None,
    where_condition: str = None,
    new_price: float = None
) -> Any:
    if operation in ["create", "update", "delete"]:
        if where_condition and operation in ["update", "delete"]:
            return execute_sales_operation_with_where(operation, where_condition,
                                                      new_quantity=new_quantity,
                                                      new_price=new_price)

        sales_cnxn = get_pg_sales_conn()
        sales_cur = sales_cnxn.cursor()

        if operation == "create":
            if not customer_id or not product_id:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ 'customer_id' and 'product_id' required for create."}

            if not validate_customer_exists(customer_id):
                sales_cnxn.close()
                return {"sql": None, "result": f"❌ Customer with ID {customer_id} not found."}

            if not validate_product_exists(product_id):
                sales_cnxn.close()
                return {"sql": None, "result": f"❌ Product with ID {product_id} not found."}

            if not unit_price:
                product_details = get_product_details(product_id)
                unit_price = product_details["price"]

            if not total_amount:
                total_amount = unit_price * quantity

            sql_query = """
                INSERT INTO sales (customer_id, product_id, quantity, unit_price, total_amount)
                VALUES (%s, %s, %s, %s, %s)
            """
            sales_cur.execute(sql_query, (customer_id, product_id, quantity, unit_price, total_amount))
            sales_cnxn.commit()

            customer_name = get_customer_name(customer_id)
            product_details = get_product_details(product_id)

            result = f"✅ Sale created: {customer_name} bought {quantity} {product_details['name']}(s) for ${total_amount:.2f}"
            sales_cnxn.close()
            return {"sql": sql_query, "result": result}

        elif operation == "update":
            if not sale_id or new_quantity is None:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ 'sale_id' and 'new_quantity' required for update."}

            sql_query = """
                UPDATE sales
                SET quantity = %s, total_amount = unit_price * %s
                WHERE id = %s
            """
            sales_cur.execute(sql_query, (new_quantity, new_quantity, sale_id))
            sales_cnxn.commit()
            result = f"✅ Sale id={sale_id} updated to quantity {new_quantity}."
            sales_cnxn.close()
            return {"sql": sql_query, "result": result}

        elif operation == "delete":
            if not sale_id:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ 'sale_id' required for delete."}

            sql_query = "DELETE FROM sales WHERE id = %s"
            sales_cur.execute(sql_query, (sale_id,))
            sales_cnxn.commit()
            result = f"✅ Sale id={sale_id} deleted."
            sales_cnxn.close()
            return {"sql": sql_query, "result": result}

    elif operation == "read":
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()

        available_columns = {
            "sale_id": "s.Id",
            "customer_first_name": "c.FirstName",
            "customer_last_name": "c.LastName",
            "product_name": "p.name",
            "product_description": "p.description",
            "quantity": "s.quantity",
            "unit_price": "s.unit_price",
            "total_price": "s.total_price",
            "sale_date": "s.sale_date",
            "customer_email": "c.Email"
        }

        default_columns = ["customer_first_name", "customer_last_name", "product_name",
                           "product_description", "quantity", "total_price", "sale_date", "customer_email"]

        if columns:
            requested_cols = [col.strip() for col in columns.split(",")]
            valid_cols = [col for col in requested_cols if col in available_columns]
            if not valid_cols:
                mysql_cnxn.close()
                return {"sql": None, "result": f"❌ Invalid columns. Available: {', '.join(available_columns.keys())}"}
            selected_columns = valid_cols
        else:
            selected_columns = default_columns

        select_parts = [f"{available_columns[col]} AS {col}" for col in selected_columns]
        where_clause = f" WHERE {where_condition}" if where_condition else ""

        sql = f"""
            SELECT {', '.join(select_parts)}
            FROM Sales s
            JOIN Customers c ON c.Id = s.customer_id
            JOIN ProductsCache p ON p.id = s.product_id
            {where_clause}
            ORDER BY s.sale_date DESC;
        """

        mysql_cur.execute(sql)
        rows = mysql_cur.fetchall()
        mysql_cnxn.close()

        processed_results = []
        for r in rows:
            sale_data = {}
            for i, col in enumerate(selected_columns):
                if col == "sale_date" and r[i]:
                    sale_data[col] = r[i]
                elif col in ["unit_price", "total_price"] and r[i] is not None:
                    sale_data[col] = float(r[i])
                else:
                    sale_data[col] = r[i]

            if display_format == "Data Format Conversion" and "sale_date" in sale_data:
                sale_data["sale_date"] = sale_data["sale_date"].strftime("%Y-%m-%d %H:%M:%S") if sale_data["sale_date"] else "N/A"
            elif display_format == "Decimal Value Formatting":
                for price_field in ["unit_price", "total_price"]:
                    if price_field in sale_data and isinstance(sale_data[price_field], (int, float)):
                        sale_data[price_field] = f"{sale_data[price_field]:.2f}"
            elif display_format == "String Concatenation":
                if "customer_first_name" in sale_data and "customer_last_name" in sale_data:
                    sale_data["customer_full_name"] = f"{sale_data['customer_first_name']} {sale_data['customer_last_name']}"
                if "product_name" in sale_data and "product_description" in sale_data:
                    sale_data["product_full_description"] = f"{sale_data['product_name']} ({sale_data['product_description'] or 'No description'})"
            elif display_format == "Null Value Removal/Handling":
                if "customer_email" in sale_data and sale_data["customer_email"] is None:
                    continue
                if "product_description" in sale_data:
                    sale_data["product_description"] = sale_data["product_description"] or "N/A"

            processed_results.append(sale_data)

        return {"sql": sql, "result": processed_results}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 10. Main: seed + run server
# ————————————————
if __name__ == "__main__":
    # 1) Create + seed all databases (if needed)
    seed_databases()

    # 2) Launch the MCP server for cloud deployment
    import os

    port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
