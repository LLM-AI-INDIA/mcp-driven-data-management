# MCP-Driven Data Management System

**Authors:**
- Mr.Jothi Periasamy
- Lokit S
- Hrushikesh A
- Samyukhtha R
- Pradyun S
- Myself


## Note
Will soon attach the link of the official repo and report here!!


This repository contains the code for a multi-tiered data management system that allows users to interact with heterogeneous databases (MySQL and PostgreSQL) using natural language. It leverages the FastMCP framework for server-side tool orchestration and integrates a Large Language Model (LLM) for intelligent query processing and response generation.
Table of Contents

1. Overview

2. Architecture

3. Key Features

4. Setup and Installation

   - Prerequisites
   - Environment Variables (.env)
   - Database Configuration (AWS RDS)
    - Running the Server
   - Running the Client
5. Usage
6. Troubleshooting

## 1. Overview

This system provides a conversational interface for performing Create, Read, Update, and Delete (CRUD) operations across multiple databases. It intelligently routes user requests to the appropriate backend tools and presents results in a user-friendly format, including advanced data formatting options.
## 2. Architecture

The system follows a layered architecture:

    - **Client Layer** (client1.py - **Streamlit UI**): The user-facing web application for natural language interaction.

    - **Application Layer** (Server_Tools1.py - **FastMCP Server**): The core orchestration layer that hosts database interaction tools, integrates with the LLM, and manages data flow between the client and databases.

    - **AI/LLM Layer** (**OpenAI API**): An external Large Language Model responsible for understanding user intent, selecting tools, extracting arguments, and generating natural language responses.

    - **Database Layer** (AWS RDS - **MySQL & PostgreSQL**): The persistent storage for customer, product, and sales data.

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
| (Server_Tools1.py)|
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
|  - ProductsCache  |
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

    Python 3.8+

    pip (Python package installer)

**Environment Variables (.env)**
Create a .env file in the root directory of your project (where Server_Tools1.py and client1.py reside) and populate it with your database credentials and API keys:
```
# OpenAI API Key for LLM integration
OPENAI_API_KEY=your_openai_api_key

# MCP Server URL (if deploying, this will be your server's public URL)
MCP_SERVER_URL=http://localhost:8000 # Change for deployment

# MySQL RDS Configuration
MYSQL_HOST=your_mysql_rds_endpoint
MYSQL_PORT=3306
MYSQL_USER=your_mysql_username
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=your_mysql_database_name

# PostgreSQL RDS Configuration
PG_HOST=your_postgresql_rds_endpoint
PG_PORT=5432
PG_DB=your_postgresql_database_name
PG_USER=your_postgresql_username
PG_PASSWORD=your_postgresql_password
```

**Database Configuration (AWS RDS)**

1. Create MySQL RDS Instance:
       - Engine: MySQL

       - Ensure "Public accessibility" is set to Yes.

       - Configure a VPC Security Group that allows Inbound TCP traffic on port 3306 from your application server's IP address (e.g., EC2 instance IP) and your local development IP address.

       - Note down the Endpoint, Port, Master Username, and Master Password.

       - Important: Your Server_Tools1.py will create the Customers, Sales, and ProductsCache tables within the database specified by MYSQL_DB.

2. Create PostgreSQL RDS Instance:

        - Engine: PostgreSQL

        - Ensure "Public accessibility" is set to Yes.

        - Configure a VPC Security Group that allows Inbound TCP traffic on port 5432 from your application server's IP address and your local development IP address.

        - Note down the Endpoint, Port, Master Username, and Master Password.

        - Important: Your Server_Tools1.py will create the products table within the database specified by PG_DB.

**Install Python Dependencies**

Navigate to your project directory in the terminal and install the required Python packages:

pip install fastmcp mysql-connector-python psycopg2-binary python-dotenv openai streamlit pandas

**Running the Server**

The server (Server_Tools1.py) hosts the database tools and communicates with the LLM.

```python Server_Tools1.py```

This command will:

    Initialize and seed your MySQL and PostgreSQL databases (dropping and recreating tables if they exist).

    Start the FastMCP server, typically listening on http://localhost:8000.

**Running the Client**

The client (client1.py) provides the Streamlit web interface.

```streamlit run client1.py```

This will open the Streamlit application in your web browser.

## 5. Usage

Once both the server and client are running, you can interact with the system through the Streamlit chat interface.

Sample Prompts:

    Create Customer: "Create a new customer named Jane Doe with email jane.doe@example.com"

    List Customers: "List all customers."

    Record Sale: "Record a sale: Customer 'Alice Smith', Product 'Widget', Quantity 5, Unit Price 9.99"

    List Sales (with formatting options):

        Select "Default Formatting" from "Display Options": "List all sales."

        Select "Data Format Conversion": "List all sales." (Date will be formatted)

        Select "Decimal Value Formatting": "List all sales." (Prices will be 2 decimal places)

        Select "String Concatenation": "List all sales." (Customer full name, product full description, sale summary will appear)

        Select "Null Value Removal/Handling": "List all sales." (Sales by "Null User" and products with null descriptions will be handled as per logic)

    Update Product Price: "Update the price of Gadget to 19.99"

    Describe Table Schema: "Describe the schema of the Sales table."

## 6. Troubleshooting

- _mysql_connector.MySQLInterfaceError: Commands out of sync: This often occurs during database seeding if multiple SQL commands are sent too rapidly on the same cursor, or if autocommit is mismanaged. The current seed_databases implementation attempts to mitigate this by separating commands and managing transactions explicitly.

        Solution: Ensure your Server_Tools1.py is the latest version from this repository. If the error persists, it might indicate a specific version incompatibility with mysql-connector-python or an underlying network latency issue during rapid database operations.

- **Database Connection Errors (e.g., "server not accessible", "timed out"):**

        Verify your .env variables are correct (host, port, user, password).

        Ensure your AWS RDS instances are "Available".

        Crucially: Check your AWS RDS Security Group inbound rules. They must allow TCP traffic on the correct port (3306 for MySQL, 5432 for PostgreSQL) from the public IP address of your EC2 instance (if deployed) and your local development machine. Your public IP can change!

        Verify "Public accessibility" is set to "Yes" for your RDS instances.

- **"No Tools Discovered" / LLM Errors:**

        Ensure your Server_Tools1.py is running and accessible at the MCP_SERVER_URL specified in your .env.

        Verify your OPENAI_API_KEY is correct and has sufficient permissions.

        Check server logs for any errors during tool registration or LLM calls.

- **"List Sales" shows no results:**

        The Sales table starts empty. You must first use a "record a sale" prompt to add transactions.

        Confirm that the "record a sale" command returns a success message, indicating the data was inserted.

        Verify that the customer_id and product_id in your sales records have matching entries in the Customers and ProductsCache tables, respectively.
