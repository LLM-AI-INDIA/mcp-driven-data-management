import os
import pyodbc
import psycopg2
from typing import Any, Optional
import pandas as pd
from datetime import datetime, date
import re
from aiohttp import ClientSession
import json

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

# ——————————————————————————————————————
# 1. MySQL Configuration
# ——————————————————————————————————————
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


# ——————————————————————————————————————
# 2. PostgreSQL Configuration (Products)
# ——————————————————————————————————————
PG_HOST = must_get("PG_HOST")
PG_PORT = int(must_get("PG_PORT"))
PG_DB = must_get("PG_DB")
PG_USER = must_get("PG_USER")
PG_PASS = must_get("PG_PASSWORD")


def get_pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",  # Supabase enforces TLS
    )
    conn.autocommit = True
    return conn


# ——————————————————————————————————————
# 3. MSSQL Configuration (CarePlan)
# ——————————————————————————————————————
MS_HOST = must_get("MS_HOST")
MS_PORT = int(must_get("MS_PORT"))
MS_DB = must_get("MS_DB")
MS_USER = must_get("MS_USER")
MS_PASS = must_get("MS_PASS")


def get_ms_conn():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={MS_HOST},{MS_PORT};"
        f"DATABASE={MS_DB};"
        f"UID={MS_USER};"
        f"PWD={MS_PASS};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    return conn


# ——————————————————————————————————————
# 4. Instantiate your MCP server
# ——————————————————————————————————————
mcp = FastMCP("SQLCRUD")

# ——————————————————————————————————————
# 5. Core ETL Tool (Combines logic for products & careplan)
# ——————————————————————————————————————
@mcp.tool()
async def etl_tool(
    database: str,
    operation: str,
    where_clause: str = None,
    limit: int = None,
    # For products create/update
    name: str = None,
    price: Optional[float] = None,
    description: str = None,
    category: str = None,
    launch_date: str = None,
    product_id: Optional[int] = None,
    new_price: Optional[float] = None,
    new_category: str = None,
    new_launch_date: str = None,
    # For careplan create/update
    client_id: Optional[int] = None,
    plan_name: str = None,
    start_date: str = None,
    end_date: str = None,
    status: str = None,
    careplan_id: Optional[int] = None,
    new_plan_name: str = None,
    new_start_date: str = None,
    new_end_date: str = None,
    new_status: str = None,
) -> Any:
    """
    Performs ETL-related operations (Read, Create, Update, Delete)
    on `products` (PostgreSQL) or `CarePlan` (MSSQL).
    """
    if database == "products":
        conn = get_pg_conn()
        cur = conn.cursor()
        table_name = "products"
    elif database == "careplan":
        conn = get_ms_conn()
        cur = conn.cursor()
        table_name = "CarePlan"
    else:
        return {"sql": None, "result": f"❌ Unknown database: '{database}'"}

    if operation == "read":
        # Handle column selection for both tables
        if database == "products":
            select_clause = "id, name, price, description, category, launch_date"
            order_by = "ORDER BY id ASC"
        else: # careplan
            select_clause = "Id, ClientId, PlanName, StartDate, EndDate, Status"
            order_by = "ORDER BY Id ASC"

        sql = f"SELECT {select_clause} FROM {table_name}"
        query_params = []
        
        if where_clause and where_clause.strip():
            if re.match(r"^[a-zA-Z0-9_=\-\s\.'\"%]+$", where_clause):
                sql += f" WHERE {where_clause}"
            else:
                return {"sql": None, "result": "❌ Invalid characters in where_clause."}

        sql += f" {order_by}"

        if limit and isinstance(limit, int):
            if database == "products":
                sql += f" LIMIT {limit}"
            else:
                sql = sql.replace("SELECT", f"SELECT TOP {limit}")

        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                conn.close()
                return {"sql": sql, "result": []}
            
            columns = [column[0] for column in cur.description]
            results = [dict(zip(columns, row)) for row in rows]
            
            conn.close()
            return {"sql": sql, "result": results}
        except Exception as e:
            conn.close()
            return {"sql": sql, "result": f"❌ SQL Error: {str(e)}"}
    elif operation == "create":
        if database == "products":
            effective_launch_date = datetime.strptime(launch_date, "%Y-%m-%d").date() if launch_date else None
            sql = "INSERT INTO products (name, price, description, category, launch_date) VALUES (%s, %s, %s, %s, %s);"
            cur.execute(sql, (name, price, description, category, effective_launch_date))
        elif database == "careplan":
            effective_start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
            effective_end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
            sql = "INSERT INTO CarePlan (ClientId, PlanName, StartDate, EndDate, Status) VALUES (?, ?, ?, ?, ?);"
            cur.execute(sql, (client_id, plan_name, effective_start_date, effective_end_date, status))
        
        conn.close()
        return {"sql": sql, "result": f"✅ New record added to {table_name}."}

    elif operation == "update":
        updates = []
        params = []
        if database == "products":
            if new_price is not None:
                updates.append("price = %s")
                params.append(new_price)
            if new_category is not None:
                updates.append("category = %s")
                params.append(new_category)
            if new_launch_date is not None:
                updates.append("launch_date = %s")
                params.append(datetime.strptime(new_launch_date, "%Y-%m-%d").date())
            
            if not updates:
                return {"sql": None, "result": f"❌ No fields provided for update for {table_name}."}
            
            sql = f"UPDATE products SET {', '.join(updates)} WHERE id = %s;"
            params.append(product_id)
        
        elif database == "careplan":
            if new_plan_name is not None:
                updates.append("PlanName = ?")
                params.append(new_plan_name)
            if new_start_date is not None:
                updates.append("StartDate = ?")
                params.append(datetime.strptime(new_start_date, "%Y-%m-%d").date())
            if new_end_date is not None:
                updates.append("EndDate = ?")
                params.append(datetime.strptime(new_end_date, "%Y-%m-%d").date())
            if new_status is not None:
                updates.append("Status = ?")
                params.append(new_status)

            if not updates:
                return {"sql": None, "result": f"❌ No fields provided for update for {table_name}."}

            sql = f"UPDATE CarePlan SET {', '.join(updates)} WHERE Id = ?;"
            params.append(careplan_id)
        
        cur.execute(sql, tuple(params) if database == "products" else params)
        conn.close()
        return {"sql": sql, "result": f"✅ Record id={product_id or careplan_id} updated in {table_name}."}

    elif operation == "delete":
        if database == "products":
            sql = "DELETE FROM products WHERE id = %s;"
            cur.execute(sql, (product_id,))
        elif database == "careplan":
            sql = "DELETE FROM CarePlan WHERE Id = ?;"
            cur.execute(sql, (careplan_id,))
        
        conn.close()
        return {"sql": sql, "result": f"✅ Record id={product_id or careplan_id} deleted from {table_name}."}

    else:
        conn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}' for {database}."}

# ——————————————————————————————————————
# 6. LLM Tool for Analysis and Visualization
# ——————————————————————————————————————
@mcp.tool()
async def analyze_and_visualize_tool(database: str, user_query: str) -> Any:
    """
    Analyzes data from a specified database and generates both text insights and a chart specification.
    """
    print(f"DEBUG: analyze_and_visualize_tool called for {database} with query: {user_query}")

    # 1. Fetch the data using the existing etl_tool
    etl_result = await etl_tool(
        database=database,
        operation="read",
        limit=1000  # Get enough data for a decent analysis
    )
    
    if "❌" in str(etl_result.get("result", "")):
        return {"error": "❌ Failed to fetch data for visualization.", "details": etl_result["result"]}

    df = pd.DataFrame(etl_result["result"])
    if df.empty:
        return {"error": "❌ No data to visualize."}
    
    # Convert DataFrame to a string (e.g., CSV) for the LLM prompt
    data_str = df.to_csv(index=False)
    
    # 2. Craft the prompt for the LLM
    prompt = f"""
    You are a data analysis expert. Your task is to analyze the provided dataset and provide key business insights, trends, and patterns.

    Based on the analysis, you must also recommend a suitable chart type and the x and y axes to best represent the data.

    The response must be in a single JSON object with the following structure:
    {{
      "analysis_text": "A markdown-formatted string with a title, a short summary, and bullet points highlighting key findings and recommendations.",
      "chart_spec": {{
        "chart_type": "string", // e.g., "bar", "line", "pie", "scatter"
        "x_axis": "string",
        "y_axis": "string"
      }}
    }}
    
    Here is the data in CSV format from the '{database}' database:
    {data_str}
    
    User's original query: "{user_query}"
    
    Please provide the analysis and chart specification in the requested JSON format.
    """

    print("DEBUG: Sending request to LLM for analysis...")
    
    # 3. Call the LLM (Gemini API)
    # The API key is managed by the environment, so we leave it empty
    apiKey = "" 
    apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={apiKey}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "analysis_text": {"type": "STRING"},
                    "chart_spec": {
                        "type": "OBJECT",
                        "properties": {
                            "chart_type": {"type": "STRING", "enum": ["bar", "line", "pie", "scatter"]},
                            "x_axis": {"type": "STRING"},
                            "y_axis": {"type": "STRING"}
                        }
                    }
                }
            }
        }
    }

    try:
        async with ClientSession() as session:
            async with session.post(apiUrl, json=payload, headers={'Content-Type': 'application/json'}) as response:
                response.raise_for_status()
                llm_response = await response.json()
                
                # Extract the JSON string from the response
                json_str = llm_response['candidates'][0]['content']['parts'][0]['text']
                analysis_data = json.loads(json_str)

                # Combine the LLM's analysis with the original data for the client
                combined_result = {
                    "status": "success",
                    "analysis_text": analysis_data["analysis_text"],
                    "chart_spec": analysis_data["chart_spec"],
                    "data": df.to_dict('records') # Send the raw data back to the client
                }
                return combined_result

    except Exception as e:
        print(f"❌ LLM API Error: {e}")
        return {"status": "error", "message": f"❌ An error occurred while generating the analysis: {e}"}

# ——————————————————————————————————————
# 7. Main
# ——————————————————————————————————————
if __name__ == "__main__":
    mcp.run()
