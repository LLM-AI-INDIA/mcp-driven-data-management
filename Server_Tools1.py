import os
import pyodbc
import psycopg2
from typing import Any
from datetime import datetime
import re

# MCP server
from fastmcp import FastMCP 
import mysql.connector
from dotenv import load_dotenv
load_dotenv() 


def must_get_clean(key: str) -> str:
    """
    Return env var with anything after a '#' stripped,
    and leading/trailing whitespace removed.
    """
    raw = os.getenv(key)
    if raw is None:
        raise RuntimeError(f"Missing required env var: {key}")
    return raw.split('#', 1)[0].strip()

# ————————————————
# 1. SQL Server Configuration
# ————————————————
MYSQL_HOST = must_get_clean("MYSQL_HOST")
MYSQL_PORT = must_get_clean("MYSQL_PORT")
MYSQL_USER = must_get_clean("MYSQL_USER")
MYSQL_PASSWORD = must_get_clean("MYSQL_PASSWORD")
MYSQL_DB = must_get_clean("MYSQL_DB")

def get_mysql_conn(db: str | None = MYSQL_DB):
    """If db is None we connect to the server only (needed to CREATE DATABASE)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        ssl_disabled=False,          # Aiven requires TLS; keep this False
        autocommit=True,
    )


# ————————————————
# 2. PostgreSQL Configuration
# ————————————————



PG_HOST = must_get_clean("PG_HOST")
PG_PORT  = must_get_clean("PG_PORT")
PG_DB   = os.getenv("PG_DB", "postgres")      # db name can default
PG_USER = must_get_clean("PG_USER")
PG_PASS = must_get_clean("PG_PASSWORD")

def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",                    # Supabase enforces TLS
    )

# ————————————————
# 3. Instantiate your MCP server
# ————————————————
mcp = FastMCP("CRUDServer")

# ————————————————
# 4. Synchronous Setup: Create & seed tables
# ————————————————
def seed_databases():
    """
    Initialise both databases and copy Postgres products into MySQL
    so the JOIN (Sales ⋈ Customers ⋈ ProductsCache) can run locally.
    """

    # ──────────────────────────────────────────────────────────────
    # 1 ─ MySQL  – create DB, Customers, Sales; seed Customers
    # ──────────────────────────────────────────────────────────────
    MYSQL_DB = must_get_clean("MYSQL_DB")

    # 1-a  create the schema if it doesn’t exist (connect with NO default DB)
    root = get_mysql_conn(db=None)
    rcur = root.cursor()
    rcur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}`;")
    rcur.close();  root.close()

    # 1-b  connect inside the target DB
    sql_cnx = get_mysql_conn()
    mcur = sql_cnx.cursor()

    # Customers
    mcur.execute("DROP TABLE IF EXISTS Customers;")
    mcur.execute("""
        CREATE TABLE Customers (
            Id        INT AUTO_INCREMENT PRIMARY KEY,
            Name      VARCHAR(100) NOT NULL,
            Email     VARCHAR(100) NOT NULL,
            CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    mcur.executemany(
        "INSERT INTO Customers (Name, Email) VALUES (%s, %s)",
        [("Alice", "alice@example.com"),
         ("Bob",   "bob@example.com")]
    )

    # Sales (starts empty)
    mcur.execute("DROP TABLE IF EXISTS Sales;")
    mcur.execute("""
        CREATE TABLE Sales (
            Id           INT AUTO_INCREMENT PRIMARY KEY,
            customer_id  INT NOT NULL,
            product_id   INT NOT NULL,
            quantity     INT NOT NULL,
            unit_price   DECIMAL(12,2) NOT NULL,
            total_price  DECIMAL(14,2) NOT NULL,
            sale_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    sql_cnx.commit()
    mcur.close();  sql_cnx.close()

    # ──────────────────────────────────────────────────────────────
    # 2 ─ PostgreSQL – create / seed products
    # ──────────────────────────────────────────────────────────────
    pg_cnx = get_pg_conn()
    pg_cnx.autocommit = True          # simplifies DDL
    pcur = pg_cnx.cursor()

    pcur.execute("DROP TABLE IF EXISTS products;")
    pcur.execute("""
        CREATE TABLE products (
            id            SERIAL PRIMARY KEY,
            name          TEXT           NOT NULL,
            price         NUMERIC(10,4)  NOT NULL,
            quantity      INTEGER        NOT NULL DEFAULT 0,
            sales_amount  NUMERIC(12,2)  NOT NULL DEFAULT 0,
            description   TEXT
        );
    """)
    pcur.executemany(
        """
        INSERT INTO products (name, price, quantity, sales_amount, description)
        VALUES (%s, %s, %s, %s, %s);
        """,
        [
            ("Widget",  9.99, 25, 9.99 * 25, "A standard widget."),
            ("Gadget", 14.99, 10, 14.99 * 10, "A useful gadget."),
        ],
    )

    # 2-b  pull the product master so we can mirror it
    pcur.execute("SELECT id, name, price, quantity, sales_amount FROM products;")
    product_rows = pcur.fetchall()    # e.g. [(1, 'Widget', 9.99), (2, 'Gadget', 14.99)]
    pcur.close();  pg_cnx.close()

    # ──────────────────────────────────────────────────────────────
    # 3 ─ Mirror products into MySQL  (ProductsCache)
    # ──────────────────────────────────────────────────────────────
    sql_cnx = get_mysql_conn()
    mcur = sql_cnx.cursor()

    mcur.execute("DROP TABLE IF EXISTS ProductsCache;")
    mcur.execute("""
        CREATE TABLE ProductsCache (
            id    INT PRIMARY KEY,
            name  VARCHAR(100) NOT NULL,
            price DECIMAL(12,4) NOT NULL,
            quantity     INT           NOT NULL DEFAULT 0,   -- ⬅️ new
            sales_amount DECIMAL(14,2) NOT NULL DEFAULT 0
        );
    """)
    mcur.executemany(
    """
    INSERT INTO ProductsCache
           (id, name, price, quantity, sales_amount)
    VALUES (%s, %s, %s, %s, %s);
    """
    , product_rows)
    sql_cnx.commit()
    mcur.close();  sql_cnx.close()

    

# ————————————————
# 5. SQL Server CRUD Tool (now with DESCRIBE)
# ————————————————
@mcp.tool()
async def sqlserver_crud(
    operation: str,
    name: str = None,
    email: str = None,
    limit: int = 10,
    customer_id: int = None,
    new_email: str = None,
    table_name: str = None,
) -> Any:
    cnxn = get_mysql_conn()        # already connected to MYSQL_DB
    cur  = cnxn.cursor()

    if operation == "create":
        if not name or not email:
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}

        sql_query = "INSERT INTO Customers (Name, Email) VALUES (%s, %s)"
        cur.execute(sql_query, (name, email))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer '{name}' added."}

    elif operation == "read":
        sql_query = """
            SELECT Id, Name, Email, CreatedAt
            FROM Customers
            ORDER BY Id ASC
        """
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {"Id": r[0], "Name": r[1], "Email": r[2], "CreatedAt": r[3].isoformat()}
            for r in rows
        ]
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not customer_id or not new_email:
            return {"sql": None, "result": "❌ 'customer_id' and 'new_email' required for update."}

        sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
        cur.execute(sql_query, (new_email, customer_id))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer id={customer_id} updated."}

    elif operation == "delete":
        if not customer_id:
            return {"sql": None, "result": "❌ 'customer_id' required for delete."}

        sql_query = "DELETE FROM Customers WHERE Id = %s"
        cur.execute(sql_query, (customer_id,))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer id={customer_id} deleted."}

    elif operation == "describe":
        # Table schema query now includes TABLE_SCHEMA to avoid cross-DB clashes
        if not table_name:
            return {"sql": None, "result": "❌ 'table_name' required for describe."}

        sql_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        cur.execute(sql_query, (MYSQL_DB, table_name))
        rows = cur.fetchall()
        result = [
            {"column": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]}
            for r in rows
        ]
        return {"sql": sql_query, "result": result}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 6. PostgreSQL CRUD Tool (now with DESCRIBE)
# ————————————————
@mcp.tool()
async def postgresql_crud(
    operation: str,
    name: str = None,
    price: float = None,
    quantity: int = None,
    sales_amount: float = None,
    description: str = None,
    limit: int = 10,
    product_id: int = None,
    new_price: float = None,
    new_quantity: int = None,
    table_name: str = None,
) -> Any:
    cnxn = get_pg_conn()
    cur  = cnxn.cursor()

    if operation == "create":
        if not name or price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'price' required for create."}

        quantity     = quantity or 0
        sales_amount = round(price * quantity, 2)

        sql = """
            INSERT INTO products (name, price, quantity, sales_amount, description)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql, (name, price, quantity, sales_amount, description))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql, "result": f"✅ Product '{name}' added."}

    elif operation == "read":
        sql = """
            SELECT id, name, price, quantity, sales_amount, description
              FROM products
             ORDER BY id ASC
        """
        cur.execute(sql)
        rows = cur.fetchall()
        cnxn.close()
        return {
            "sql": sql,
            "result": [
                {
                    "id": r[0],
                    "name": r[1],
                    "price": float(r[2]),
                    "quantity": r[3],
                    "sales_amount": float(r[4]),
                    "description": r[5] or "",
                }
                for r in rows
            ],
        }

    elif operation == "update":
        if not product_id or (new_price is None and new_quantity is None):
            cnxn.close()
            return {
                "sql": None,
                "result": "❌ 'product_id' and ≥1 of 'new_price'/'new_quantity' required.",
            }

        # Grab current values first
        cur.execute("SELECT price, quantity FROM products WHERE id = %s", (product_id,))
        row = cur.fetchone()
        if not row:
            cnxn.close()
            return {"sql": None, "result": f"❌ Product id={product_id} not found."}

        curr_price, curr_qty = float(row[0]), row[1]
        eff_price = new_price    if new_price    is not None else curr_price
        eff_qty   = new_quantity if new_quantity is not None else curr_qty
        new_sales = round(eff_price * eff_qty, 2)

        sql = """
            UPDATE products
               SET price        = %s,
                   quantity     = %s,
                   sales_amount = %s
             WHERE id = %s
        """
        cur.execute(sql, (eff_price, eff_qty, new_sales, product_id))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql, "result": f"✅ Product id={product_id} updated."}

    elif operation == "delete":
        if not product_id and not name:
            cnxn.close()
            return {"sql": None, "result": "❌ Need 'product_id' **or** 'name'."}

        if product_id:
            sql = "DELETE FROM products WHERE id = %s"
            params = (product_id,)
        else:
            sql = "DELETE FROM products WHERE name = %s"
            params = (name,)

        cur.execute(sql, params)
        cnxn.commit()
        cnxn.close()
        return {"sql": sql, "result": "✅ Deleted product."}

    elif operation == "describe":
        if not table_name:
            cnxn.close()
            return {"sql": None, "result": "❌ 'table_name' required for describe."}

        sql = """
            SELECT column_name, data_type, is_nullable, character_maximum_length
              FROM information_schema.columns
             WHERE table_name = %s
        """
        cur.execute(sql, (table_name,))
        rows = cur.fetchall()
        cnxn.close()
        return {
            "sql": sql,
            "result": [
                {
                    "column": r[0],
                    "type": r[1],
                    "nullable": r[2],
                    "max_length": r[3],
                }
                for r in rows
            ],
        }

    cnxn.close()
    return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}



@mcp.tool()
async def sales_crud(
    operation: str,
    customer_name: str = None,
    product_name: str  = None,
    customer_id:   int  = None,
    email: str=None,
    product_id:    int  = None,
    quantity:      int  = None,
    unit_price:   float = None,
    sale_id:       int  = None,
    total_price:  float = None,
    total_amount: float = None
) -> Any:
    # 1) Resolve IDs & prices
    mysql = get_mysql_conn();    mcur = mysql.cursor()
    pg    = get_pg_conn();       pcur = pg.cursor()

    # Fetch customer_id if name given
    
    if operation in {"read", "describe"}:
        pass
    else:
        if customer_name:
            mcur.execute(
               "SELECT Id FROM Customers WHERE Name = %s LIMIT 1",                (customer_name,),
            )
            row = mcur.fetchone()
            if row:
                customer_id = row[0]
            else:
                email = (email or f"{re.sub(r'[^a-z0-9]+', '', customer_name.lower())}@example.com")
                mcur.execute(
                    "INSERT INTO Customers (Name, Email) VALUES (%s, %s)",
                    (customer_name, email),
                )
                mysql.commit()
                customer_id = mcur.lastrowid

        if product_name:
            pcur.execute(
                "SELECT id FROM products WHERE name = %s LIMIT 1",
                (product_name,),
            )
            row = pcur.fetchone()
            if row:
                product_id = row[0]

            else:
                pcur.execute(
                    "INSERT INTO products (name, price) VALUES (%s, %s) RETURNING id",
                    (product_name, unit_price),
                )
                product_id = pcur.fetchone()[0]
                pg.commit()
            mcur.execute(
        """
        INSERT INTO ProductsCache
               (id, name, price, quantity, sales_amount)
        VALUES (%s, %s, %s, 0, 0)
        ON DUPLICATE KEY UPDATE
            name  = VALUES(name),
            price = VALUES(price)
        """,
        (product_id, product_name, unit_price)
        )
            mysql.commit()
    # CREATE
    if operation == "create":
        if total_price is None and total_amount is not None:
            total_price = total_amount
        if total_price is None and quantity is not None and unit_price is not None:
            total_price = quantity * unit_price
        if not all([customer_id, product_id, quantity, unit_price]):
            return {"sql": None, "result": "❌ 'customer_id|Name', 'product_id|Name', 'quantity' and 'unit_price' required."}
        total = round(quantity * unit_price, 2)
        sql = ( "INSERT INTO Sales (customer_id, product_id, quantity, unit_price, total_price) "
                "VALUES (%s, %s, %s, %s, %s)" )
        mcur.execute(sql, (customer_id, product_id, quantity, unit_price, total))
        mysql.commit()
        pcur.execute(
            """
            UPDATE products
               SET quantity     = quantity + %s,
                   sales_amount = sales_amount + %s
             WHERE id = %s
            """,
            (quantity, total, product_id),
        )
        mysql.commit()
        pg.commit()
        return {
            "sql": sql,
            "result": f"✅ Sale recorded: cust={customer_id}, "
                      f"prod={product_id}, qty={quantity}, total={total}"
        }

    # READ
    elif operation == "read":
        mysql = get_mysql_conn()
        mcur  = mysql.cursor()  
        sql = """ SELECT * FROM Sales;"""      
        mcur.execute(sql)
        rows = mcur.fetchall();
        if rows == " ": print("select * from sales is not working")
        sql = """
        SELECT  s.Id,
        c.Name      AS customer,
        p.name      AS product,
        s.quantity,
        s.unit_price,
        s.total_price,
        s.sale_date
FROM    Sales          s
JOIN    Customers      c ON c.Id = s.customer_id
JOIN    ProductsCache  p ON p.id = s.product_id
ORDER BY s.sale_date DESC;
    """
        mcur.execute(sql)
        rows = mcur.fetchall()

        mcur.close();  mysql.close()

        result = [
        {
            "sale_id":     r[0],
            "customer":    r[1],
            "product":     r[2],
            "quantity":    r[3],
            "unit_price":  float(r[4]),
            "total_price": float(r[5]),
            "sale_date":   r[6].isoformat() if r[6] else None,
        }
        for r in rows
        ]
        #return {"sql": sql, "result": result}


        '''result.append({
                "sale_id":     sale_id,
                "customer":    customer,
                "product":     prod_map.get(prod_id, f"id={prod_id}"),
                "quantity":    qty,
                "unit_price":  float(u_price),
                "total_price": float(t_price),
                "sale_date":   s_date.isoformat(),
            })'''

        return {"sql": sql, "result": result}

    # UPDATE (only quantity or unit_price)
    elif operation == "update":
        if not sale_id or (quantity is None and unit_price is None):
            return {"sql": None, "result": "❌ 'sale_id' and ≥1 of 'quantity'/'unit_price' required."}
        # fetch current
        mcur.execute("SELECT quantity, unit_price FROM Sales WHERE Id=%s", (sale_id,))
        cur_qty, cur_price = mcur.fetchone()
        new_qty   = quantity    if quantity    is not None else cur_qty
        new_price = unit_price  if unit_price  is not None else cur_price
        total     = round(new_qty * new_price, 2)
        sql = """
            UPDATE Sales
               SET quantity    = %s,
                   unit_price  = %s,
                   total_price = %s
             WHERE Id = %s
        """
        mcur.execute(sql, (new_qty, new_price, total, sale_id))
        mysql.commit()
        return {"sql": sql, "result": f"✅ Sale {sale_id} updated."}

    # DELETE
    elif operation == "delete":
        if not sale_id:
            return {"sql": None, "result": "❌ 'sale_id' required for delete."}
        sql = "DELETE FROM Sales WHERE Id = %s"
        mcur.execute(sql, (sale_id,))
        mysql.commit()
        return {"sql": sql, "result": f"✅ Sale {sale_id} deleted."}

    # DESCRIBE
    elif operation == "describe":
        sql = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = 'Sales'
        """
        mcur.execute(sql)
        rows = mcur.fetchall()
        schema = [{
            "column":  r[0],
            "type":    r[1],
            "nullable":r[2],
            "max_len":r[3]
        } for r in rows]
        return {"sql": sql, "result": schema}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 7. Main: seed + run server
# ————————————————
if __name__ == "__main__":
    # 1) Create + seed both databases
    seed_databases()

    # 2) Launch the MCP server with Streamable HTTP at /streamable-http
    mcp.run(transport="streamable-http")
