# MCP-Driven Data Management System

This repository contains the code for a multi-tiered data management system that allows users to interact with heterogeneous databases (MySQL and PostgreSQL) using natural language. It leverages the FastMCP framework for server-side tool orchestration and integrates a Large Language Model (LLM) for intelligent query processing and response generation.
Table of Contents

1. Overview

2. Architecture

3. Key Features

4. Setup and Installation

   - Prerequisites
   - Environment Variables (.env)
   - Database Configuration (Aiven Console for MySQL and Supabase for Postgres)
    - Running the Server
   - Running the Client
5. Usage
6. Troubleshooting

## 1. Overview

This system provides a conversational interface for performing Create, Read, Update, and Delete (CRUD) operations across multiple databases. It intelligently routes user requests to the appropriate backend tools and presents results in a user-friendly format, including advanced data formatting options.
## 2. Architecture

The system follows a layered architecture:

**Client Layer** (client1.py - **Streamlit UI**): The user-facing web application for natural language interaction.

**Application Layer** (main1.py - **FastMCP Server**): The core orchestration layer that hosts database interaction tools, integrates with the LLM, and manages data flow between the client and databases.

**AI/LLM Layer** (**OpenAI API**): An external Large Language Model responsible for understanding user intent, selecting tools, extracting arguments, and generating natural language responses.

**Database Layer** (Aiven console & Supabase - **MySQL & PostgreSQL**): The persistent storage for customer, product, and sales data.

**Conceptual Diagram:**

```
+-------------------+
|   User (Human)    |
+---------+---------+
          |
          | (Types Query)
          v
+-------------------+
|   Client Layer    |
|   (client1.py)    |
|   (Streamlit UI)  |
+---------+---------+
          |
          | (Async HTTP Request)
          v
+-------------------+
|  Application Layer|
| (main1.py)|
|  (FastMCP Server) |
+---------+---------+
          |  ^
          |  | (API Calls)
          |  |
          |  v
+---------+---------+
|   AI / LLM Layer  |
|   (OpenAI API)    |
+-------------------+
          |  ^
          |  | (DB Connections & Queries)
          |  |
          |  v
+---------+---------+
|   Database Layer  |
+---------+---------+
|  MySQL RDS        |
|  - Customers      |
|  - Sales          |
|  - Careplan       |
+-------------------+
|  PostgreSQL RDS   |
|  - products       |
+-------------------+

```

## 3. Key Features

    - Natural Language Interface: Interact with databases using conversational prompts.

    - Intelligent Tool Routing: LLM-powered selection of the correct database tool (sqlserver_crud, postgresql_crud, sales_crud) based on user intent.

    - Heterogeneous Database Support: Manages data across MySQL (for Customers, Sales, ProductsCache) and PostgreSQL (for master products data).

    - Cross-Database Operations: The sales_crud tool performs conceptual "joins" by linking sales records (MySQL) with customer data (MySQL) and product details (MySQL ProductsCache, mirrored from PostgreSQL).

    - Dynamic Data Formatting: Client-side dropdown allows users to select how sales data is displayed:

        - - Data Format Conversion: Formats sale_date for readability.

        - - Decimal Value Formatting: Limits prices to two decimal places.

        - - String Concatenation: Combines customer names and product details into single fields.

        - - Null Value Removal/Handling: Demonstrates filtering out records with null values or replacing nulls with placeholders.

    - Asynchronous Communication: Efficient, non-blocking client-server and server-database interactions using asyncio.

    - Automated Database Seeding: The server automatically sets up and populates necessary tables on startup.

## 4. Setup and Installation
**Prerequisites**

    Python 3.10+

    pip (Python package installer)

**Environment Variables (.env)**
Create a .env file in the root directory of your project (where main1.py and client1.py reside) and populate it with your database credentials and API keys:
```
# OpenAI API Key for LLM integration
GROQ_API_KEY=your_api_key

# MCP Server URL (if deploying, this will be your server's public URL)
MCP_SERVER_URL=http://localhost:8000 # Change for deployment --> it will be the public URL for AWS EC2 instance and the onrender.com link for the Render Web service.
!!!!ADD THE PORT NUMBER AFTER THE URL --->    <url>:8000/


# MySQL Configuration
MYSQL_HOST=your_mysql_rds_endpoint
MYSQL_PORT=3306
MYSQL_USER=your_mysql_username
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=your_mysql_database_name

# PostgreSQL Configuration
PG_HOST=your_postgresql_rds_endpoint
PG_PORT=5432
PG_DB=your_postgresql_database_name
PG_USER=your_postgresql_username
PG_PASSWORD=your_postgresql_password

PG_SALES_HOST=your_postgresql_rds_endpoint
PG_SALES_PORT=5432
PG_SALES_DB=your_postgresql_database_name
PG_SALES_USER=your_postgresql_username
PG_SALES_PASSWORD=your_postgresql_password
```

**Database Configuration (AWS RDS)**

1. Create MySQL on Aiven Console
2. Create PostgreSQL Supabase Instance

> Note down their endpoint, IP address, password, host name, username, and DB name!!! Check the `.env` file format above ☝️

**Install Python Dependencies**

Navigate to your project directory in the terminal and install the required Python packages:

```pip install fastmcp mysql-connector-python psycopg2-binary groq uvicorn pyodbc streamlit pandas pillow openai fastmcp mcp mcp-server python-dotenv asyncio langchain_groq langchain_core plotly```

**Running the Server**

Go to [Render](render.com) and sign up. Then create a **Web Service**

While setting it up, give your repository's (the one with the server code) public link. 

The server (main1.py) hosts the database tools and communicates with the LLM.

```nohup python -u main1.py > server.log 2>&1 &```

This command will:

    Initialize and seed your MySQL and PostgreSQL databases (dropping and recreating tables if they exist).

    Start the FastMCP server, typically listening on http://localhost:8000.

**Running the Client**

The client (client1.py) provides the Streamlit web interface. We are currently running it using the Streamlit cloud.

```streamlit run client1.py```

This will open the Streamlit application in your web browser.

## 5. Usage

Once both the server and client are running, you can interact with the system through the Streamlit chat interface.

Sample Prompts:
- **"Add a new customer named John Smith with email john.smith@email.com"**
- **"Update the price of Widget to $29.99"**
- **"Delete customer Alice Johnson from the database"**
- **"Create a new product called 'Premium Gadget' priced at $149.99 with description 'High-end gadget'"**
- **"Change Bob Wilson's email to bob.wilson@newcompany.com"**
- **"Show me all sales data with a bar chart of total sales by product"**
- **"Display customer information with a pie chart showing distribution by first letter of name"**
- **"List all products with prices and create a histogram of price distribution"**
- **"Show sales trends over time with a line chart"**
- **"Show me sales where total price is greater than 50 with a visualization"**
- **"Display only customer names and emails, excluding other fields"**
- **"List products with prices between $20 and $50, sorted by price"**
- **"Show me the highest selling product by quantity"**
- **"Show me customers who haven't made any purchases yet"**
- **"Visualize sales distribution by time of day"**
- **”Show me the sales data in a visual form”**

## 6. Troubleshooting

-> _mysql_connector.MySQLInterfaceError: Commands out of sync: This often occurs during database seeding if multiple SQL commands are sent too rapidly on the same cursor, or if autocommit is mismanaged. The current seed_databases implementation attempts to mitigate this by separating commands and managing transactions explicitly.

-> If you are unable to use the `pyodbc` library, depending on the OS of the deployment machine - use the appropriate cmd. Check [here](https://stackoverflow.com/questions/2960339/unable-to-install-pyodbc-on-linux)

-> For UI issues, check your `client1.py` file

- **Database Connection Errors (e.g., "server not accessible", "timed out"):**

        Verify your .env variables are correct (host, port, user, password, URL).

        Ensure your DB instances are "Available".

- **"No Tools Discovered" / LLM Errors:**

        Ensure your main1.py is running and accessible at the MCP_SERVER_URL specified in your .env.

        Verify your GROQ_API_KEY is correct and has sufficient permissions.

        Check server logs for any errors during tool registration or LLM calls.

- **"List Sales" shows no results:**

        The Sales table starts empty. You must first use a "record a sale" prompt to add transactions.

        Confirm that the "record a sale" command returns a success message, indicating the data was inserted.

        Verify that the customer_id and product_id in your sales records have matching entries in the Customers and ProductsCache tables, respectively.
