import os
import pyodbc
import psycopg2
from typing import Any, Optional
import random
from datetime import datetime, timedelta
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
    sql_cur.execute("DROP TABLE IF EXISTS CarePlan;")
    sql_cur.execute("DROP TABLE IF EXISTS CallLogs;")

    # Re-enable foreign key checks
    sql_cur.execute("SET FOREIGN_KEY_CHECKS = 1;")

    ##### Create Customers table with FirstName and LastName
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

    ##### Create Sales table in MySQL with foreign key constraints
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


    ##### Create CarePlan table in MySQL
    sql_cur.execute("""
    CREATE TABLE IF NOT EXISTS CarePlan (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        -- Base Information
        ActualReleaseDate DATE,
        NameOfYouth VARCHAR(255),
        RaceEthnicity VARCHAR(100),
        MediCalIDNumber VARCHAR(50),
        ResidentialAddress TEXT,
        Telephone VARCHAR(20),
        MediCalHealthPlan VARCHAR(100),

        -- Health Information
        HealthScreenings TEXT,
        HealthAssessments TEXT,
        ChronicConditions TEXT,
        PrescribedMedications TEXT,

        -- Reentry Specific Fields
        Screenings TEXT,
        ClinicalAssessments TEXT,
        TreatmentHistory TEXT,
        PrimaryPhysicianContacts TEXT,
        ScheduledAppointments TEXT,
        Housing TEXT,
        Employment TEXT,
        IncomeBenefits TEXT,
        FoodClothing TEXT,
        Transportation TEXT,
        IdentificationDocuments TEXT,
        LifeSkills TEXT,
        FamilyChildren TEXT,
        EmergencyContacts TEXT,
        CourtDates TEXT,
        ServiceReferrals TEXT,
        HomeModifications TEXT,
        DurableMedicalEquipment TEXT,

        -- Metadata
        CarePlanType VARCHAR(50) DEFAULT 'General',
        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        Status VARCHAR(50) DEFAULT 'Active',
        Notes TEXT
    );
    """)

    # Sample data for 100 patients with realistic information
    care_plan_data = []

    # Common data for realistic patient profiles
    first_names = ['James', 'Maria', 'David', 'Sarah', 'Michael', 'Jessica', 'Christopher', 'Ashley',
                  'Matthew', 'Amanda', 'Joshua', 'Jennifer', 'Daniel', 'Elizabeth', 'Andrew', 'Michelle',
                  'Anthony', 'Emily', 'Robert', 'Nicole', 'William', 'Samantha', 'Joseph', 'Stephanie',
                  'Thomas', 'Rebecca', 'Charles', 'Laura', 'John', 'Kimberly', 'Ryan', 'Amy', 'Nicholas',
                  'Angela', 'Kevin', 'Crystal', 'Brian', 'Heather', 'Jason', 'Melissa', 'Eric', 'Tiffany',
                  'Adam', 'Christina', 'Jonathan', 'Rachel', 'Justin', 'Kelly', 'Timothy', 'Lisa']

    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez',
                 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor',
                 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez',
                 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King', 'Wright',
                 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall',
                 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts']

    races = ['Caucasian', 'African American', 'Hispanic/Latino', 'Asian', 'Native American',
            'Pacific Islander', 'Multiracial', 'Other']

    health_plans = ['Anthem Blue Cross', 'Kaiser Permanente', 'Health Net', 'Blue Shield of California',
                   'Molina Healthcare', 'LA Care Health Plan', 'Community Health Group', 'Inland Empire Health Plan']

    chronic_conditions = ['Asthma', 'Diabetes', 'Hypertension', 'Depression', 'Anxiety', 'ADHD',
                         'Bipolar Disorder', 'PTSD', 'Substance Use Disorder', 'HIV/AIDS', 'Hepatitis C',
                         'Epilepsy', 'Arthritis', 'Heart Disease', 'COPD']

    medications = ['Metformin', 'Lisinopril', 'Albuterol', 'Sertraline', 'Escitalopram', 'Bupropion',
                  'Risperidone', 'Aripiprazole', 'Quetiapine', 'Lamotrigine', 'Valproic Acid', 'Insulin',
                  'Amlodipine', 'Atorvastatin', 'Metoprolol', 'Gabapentin', 'Trazodone', 'Clonazepam']

    housing_options = ['Stable Housing', 'Transitional Housing', 'Homeless', 'Shelter', 'Living with Family',
                      'Group Home', 'Independent Living', 'Supportive Housing']

    employment_status = ['Employed Full-time', 'Employed Part-time', 'Unemployed', 'Student', 'Disabled',
                        'Vocational Training', 'Seeking Employment', 'Volunteer']

    # Generate 100 realistic patient records
    for i in range(100):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        full_name = f"{first_name} {last_name}"

        # Generate realistic data for each field
        care_plan_data.append((
            # ActualReleaseDate (within last 2 years)
            datetime.now().date() - timedelta(days=random.randint(0, 730)),
            full_name,
            random.choice(races),
            f"MC{random.randint(1000000, 9999999)}",  # MediCal ID
            f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Pine'])} St, "
            f"{random.choice(['Los Angeles', 'San Diego', 'San Francisco', 'Sacramento', 'Oakland', 'Fresno'])} CA",
            f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            random.choice(health_plans),

            # HealthScreenings
            f"Last screening: {random.choice(['2023', '2024'])}; "
            f"Results: {random.choice(['Normal', 'Abnormal - follow up needed', 'Pending'])}",

            # HealthAssessments
            f"Comprehensive assessment completed; Risk level: {random.choice(['Low', 'Medium', 'High'])}",

            # ChronicConditions (1-3 conditions)
            ", ".join(random.sample(chronic_conditions, random.randint(1, 3))),

            # PrescribedMedications (1-4 medications)
            ", ".join(random.sample(medications, random.randint(1, 4))),

            # Screenings
            f"Mental health: {random.choice(['Completed', 'Pending'])}; "
            f"Substance use: {random.choice(['Completed', 'Pending'])}; "
            f"Physical health: {random.choice(['Completed', 'Pending'])}",

            # ClinicalAssessments
            f"PHQ-9: {random.randint(0, 27)}; GAD-7: {random.randint(0, 21)}; "
            f"SUD assessment: {random.choice(['Positive', 'Negative', 'Inconclusive'])}",

            # TreatmentHistory
            f"Previous treatment: {random.choice(['Outpatient', 'Inpatient', 'Residential', 'None'])}; "
            f"Duration: {random.randint(1, 24)} months",

            # PrimaryPhysicianContacts
            f"Dr. {random.choice(['Brown', 'Wilson', 'Chen', 'Garcia', 'Patel'])}; "
            f"Phone: {random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",

            # ScheduledAppointments
            f"Next appointment: {(datetime.now() + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')}; "
            f"Type: {random.choice(['Therapy', 'Medical', 'Psychiatric', 'Case Management'])}",

            # Housing
            random.choice(housing_options),

            # Employment
            random.choice(employment_status),

            # IncomeBenefits
            f"Income: ${random.randint(500, 3000)}/month; "
            f"Benefits: {random.choice(['Medi-Cal', 'SSI', 'SSDI', 'CalFresh', 'None'])}",

            # FoodClothing
            f"Food security: {random.choice(['Secure', 'Insecure'])}; "
            f"Clothing needs: {random.choice(['Adequate', 'Needs assistance'])}",

            # Transportation
            random.choice(['Public transit', 'Personal vehicle', 'Rideshare', 'Walking', 'Bicycle']),

            # IdentificationDocuments
            f"ID status: {random.choice(['Has all documents', 'Missing some', 'No documents'])}; "
            f"Needs: {random.choice(['State ID', 'Birth certificate', 'Social Security card', 'None'])}",

            # LifeSkills
            f"Skills assessment: {random.choice(['Basic skills present', 'Needs training', 'Advanced skills'])}; "
            f"Focus areas: {random.choice(['Budgeting', 'Cooking', 'Job search', 'Communication'])}",

            # FamilyChildren
            f"Family support: {random.choice(['Strong', 'Moderate', 'Limited', 'None'])}; "
            f"Dependents: {random.randint(0, 4)} children",

            # EmergencyContacts
            f"Contact: {random.choice(['Parent', 'Sibling', 'Friend', 'Case worker'])} "
            f"{random.choice(first_names)} {random.choice(last_names)}; "
            f"Phone: {random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",

            # CourtDates
            f"Next court date: {(datetime.now() + timedelta(days=random.randint(7, 90))).strftime('%Y-%m-%d')}; "
            f"Type: {random.choice(['Progress review', 'Hearing', 'Sentencing', 'Probation'])}",

            # ServiceReferrals
            f"Referrals: {random.choice(['Mental health', 'Substance abuse', 'Vocational', 'Educational', 'Housing'])}; "
            f"Status: {random.choice(['Pending', 'Completed', 'In progress'])}",

            # HomeModifications
            random.choice(['None needed', 'Ramp installation', 'Grab bars', 'Widened doorways', 'Accessible bathroom']),

            # DurableMedicalEquipment
            random.choice(['None', 'Wheelchair', 'Walker', 'Oxygen tank', 'CPAP machine', 'Prosthetics']),

            # CarePlanType - 60% Reentry, 40% General
            random.choices(['Reentry Care Plan', 'General Care Plan'], weights=[60, 40])[0],

            # Status
            random.choice(['Active', 'Completed', 'On hold', 'Transferred']),

            # Notes
            f"Patient demonstrates {random.choice(['good', 'fair', 'poor'])} progress. "
            f"Key challenges: {random.choice(['housing stability', 'employment', 'mental health', 'substance use', 'family reunification'])}. "
            f"Strengths: {random.choice(['motivation', 'family support', 'work ethic', 'resilience', 'communication skills'])}."
        ))

    # Insert the sample care plan data
    sql_cur.executemany("""
        INSERT INTO CarePlan (ActualReleaseDate, NameOfYouth, RaceEthnicity, MediCalIDNumber, ResidentialAddress, Telephone,
            MediCalHealthPlan, HealthScreenings, HealthAssessments, ChronicConditions, PrescribedMedications,
            Screenings, ClinicalAssessments, TreatmentHistory, PrimaryPhysicianContacts, ScheduledAppointments,
            Housing, Employment, IncomeBenefits, FoodClothing, Transportation, IdentificationDocuments,
            LifeSkills, FamilyChildren, EmergencyContacts, CourtDates, ServiceReferrals, HomeModifications,
            DurableMedicalEquipment, CarePlanType, Status, Notes
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, care_plan_data)


    ##### Creating the CallLogs Table
    sql_cur.execute("""
        CREATE TABLE IF NOT EXISTS CallLogs (
            LogID INT AUTO_INCREMENT PRIMARY KEY,
            CallDate DATETIME NOT NULL,
            CustomerID INT,
            AgentName VARCHAR(100),
            CallDuration INT, -- in seconds
            CallType VARCHAR(50), -- inbound/outbound/transfer
            CallStatus VARCHAR(50), -- completed/dropped/voicemail
            IssueCategory VARCHAR(100),
            ResolutionStatus VARCHAR(50), -- resolved/escalated/pending
            SentimentScore DECIMAL(3,2), -- -1.00 to 1.00
            CallNotes TEXT,
            WaitTime INT, -- in seconds
            TransferCount INT DEFAULT 0,
            FOREIGN KEY (CustomerID) REFERENCES Customers(Id) ON DELETE SET NULL,
            INDEX idx_call_date (CallDate),
            INDEX idx_customer (CustomerID),
            INDEX idx_category (IssueCategory)
        );
    """)

    call_log_data = []
    agents = ['Sarah Chen', 'Mike Johnson', 'Emily Davis', 'James Wilson', 'Lisa Anderson', 'David Martinez', 'Jennifer Brown', 'Robert Taylor']
    call_types = ['inbound', 'outbound', 'transfer']
    call_statuses = ['completed', 'dropped', 'voicemail']
    issue_categories = ['billing', 'technical', 'product_inquiry', 'complaint', 'order_status', 'account', 'refund', 'general']
    resolution_statuses = ['resolved', 'escalated', 'pending', 'follow_up']

    base_date = datetime.now() - timedelta(days=90)

    for i in range(300):
        call_date = base_date + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(8, 20),
            minutes=random.randint(0, 59)
        )

        call_log_data.append((
            call_date,
            random.randint(1, 3),  # CustomerID (1-3 from existing customers)
            random.choice(agents),
            random.randint(30, 1800),  # CallDuration (30 sec to 30 min)
            random.choice(call_types),
            random.choice(call_statuses),
            random.choice(issue_categories),
            random.choice(resolution_statuses),
            round(random.uniform(-0.5, 1.0), 2),  # SentimentScore
            f"Customer called regarding {random.choice(issue_categories)} issue. {random.choice(['Issue resolved successfully.', 'Escalated to supervisor.', 'Follow-up required.', 'Customer satisfied with resolution.'])}",
            random.randint(0, 300),  # WaitTime (0-5 minutes)
            random.randint(0, 3)  # TransferCount
        ))

    sql_cur.executemany("""
        INSERT INTO CallLogs (CallDate, CustomerID, AgentName, CallDuration, CallType,
                             CallStatus, IssueCategory, ResolutionStatus, SentimentScore,
                             CallNotes, WaitTime, TransferCount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, call_log_data)

    sql_cnx.close()

    # ---------- PostgreSQL (Products) ----------
    pg_cnxn = get_pg_conn()
    pg_cnxn.autocommit = True
    pg_cur = pg_cnxn.cursor()

    pg_cur.execute("DROP TABLE IF EXISTS products CASCADE;")

    ##### Create table for Products in Postgres
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
def get_customer_id_by_name(name: str) -> Optional[int]:
    conn = get_mysql_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT Id FROM Customers WHERE Name = %s", (name,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def get_product_id_by_name(name: str) -> Optional[int]:
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM Products WHERE name = %s", (name,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

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


def find_customer_by_name_enhanced(name: str) -> dict:
    """Enhanced customer search that handles multiple matches intelligently"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()

        # Search strategy with priorities:
        # 1. Exact full name match (case insensitive)
        # 2. Exact first name or last name match
        # 3. Partial name matches

        all_matches = []

        # 1. Try exact full name match (case insensitive)
        mysql_cur.execute("SELECT Id, Name, Email FROM Customers WHERE LOWER(Name) = LOWER(%s)", (name,))
        exact_matches = mysql_cur.fetchall()

        if exact_matches:
            # If only one exact match, return it immediately
            if len(exact_matches) == 1:
                mysql_cnxn.close()
                return {
                    "found": True,
                    "multiple_matches": False,
                    "customer_id": exact_matches[0][0],
                    "customer_name": exact_matches[0][1],
                    "customer_email": exact_matches[0][2]
                }
            else:
                # Multiple exact matches (rare but possible)
                for match in exact_matches:
                    all_matches.append({
                        "id": match[0],
                        "name": match[1],
                        "email": match[2],
                        "match_type": "exact_full_name"
                    })

        # 2. Try exact first name or last name match if no exact full name match
        if not exact_matches:
            mysql_cur.execute("""
                SELECT Id, Name, Email FROM Customers
                WHERE LOWER(FirstName) = LOWER(%s)
                   OR LOWER(LastName) = LOWER(%s)
            """, (name, name))
            name_matches = mysql_cur.fetchall()

            for match in name_matches:
                all_matches.append({
                    "id": match[0],
                    "name": match[1],
                    "email": match[2],
                    "match_type": "exact_name_part"
                })

        # 3. Try partial matches only if no exact matches found
        if not all_matches:
            mysql_cur.execute("""
                SELECT Id, Name, Email FROM Customers
                WHERE LOWER(Name) LIKE LOWER(%s)
                   OR LOWER(FirstName) LIKE LOWER(%s)
                   OR LOWER(LastName) LIKE LOWER(%s)
            """, (f"%{name}%", f"%{name}%", f"%{name}%"))
            partial_matches = mysql_cur.fetchall()

            for match in partial_matches:
                all_matches.append({
                    "id": match[0],
                    "name": match[1],
                    "email": match[2],
                    "match_type": "partial"
                })

        mysql_cnxn.close()

        # Handle results
        if not all_matches:
            return {"found": False, "error": f"Customer '{name}' not found"}

        if len(all_matches) == 1:
            match = all_matches[0]
            return {
                "found": True,
                "multiple_matches": False,
                "customer_id": match["id"],
                "customer_name": match["name"],
                "customer_email": match["email"]
            }

        # Multiple matches found
        return {
            "found": True,
            "multiple_matches": True,
            "matches": all_matches,
            "error": f"Multiple customers found matching '{name}'"
        }

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


# ————————————————
# 7. Enhanced MySQL CRUD Tool (Customers) with Smart Name Resolution
# ————————————————
# Fixed sqlserver_crud function with proper variable initialization
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
    """Manages customer data in the MySQL database. Use for creating, reading, updating, or deleting customers."""
    cnxn = get_mysql_conn()
    cur = cnxn.cursor()

    if operation == "create":
        if not name or not email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}

        # NEW LOGIC: Check if customer with this name already exists
        # Search for existing customers with the same first name or full name
        search_name = name.strip()

        # Check for exact name matches or first name matches
        cur.execute("""
            SELECT Id, Name, Email FROM Customers
            WHERE LOWER(Name) = LOWER(%s)
               OR LOWER(FirstName) = LOWER(%s)
               OR LOWER(Name) LIKE LOWER(%s)
        """, (search_name, search_name, f"%{search_name}%"))

        existing_customers = cur.fetchall()

        if existing_customers:
            # Filter out customers who already have emails
            customers_without_email = [c for c in existing_customers if not c[2]]  # c[2] is Email
            customers_with_email = [c for c in existing_customers if c[2]]  # c[2] is Email

            if len(existing_customers) == 1:
                # Only one customer found
                existing_customer = existing_customers[0]
                if existing_customer[2]:  # Already has email
                    cnxn.close()
                    return {"sql": None, "result": f"ℹ️ Customer '{existing_customer[1]}' already has email '{existing_customer[2]}'. If you want to update it, please specify the full name."}
                else:
                    # Customer exists but no email, update with the email
                    sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
                    cur.execute(sql_query, (email, existing_customer[0]))
                    cnxn.commit()
                    cnxn.close()
                    return {"sql": sql_query, "result": f"✅ Email '{email}' added to existing customer '{existing_customer[1]}'."}

            elif len(existing_customers) > 1:
                # Multiple customers found - ask for clarification
                customer_list = []
                for c in existing_customers:
                    email_status = f"(has email: {c[2]})" if c[2] else "(no email)"
                    customer_list.append(f"- {c[1]} {email_status}")

                customer_details = "\n".join(customer_list)
                cnxn.close()
                return {"sql": None, "result": f"❓ Multiple customers found with name '{search_name}':\n{customer_details}\n\nPlease specify the full name (first and last name) to identify which customer you want to add the email to, or use a different name if you want to create a new customer."}

        # No existing customer found, create new customer
        # Split name into first and last name (simple split)
        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        sql_query = "INSERT INTO Customers (FirstName, LastName, Name, Email) VALUES (%s, %s, %s, %s)"
        cur.execute(sql_query, (first_name, last_name, name, email))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ New customer '{name}' created with email '{email}'."}
    elif operation == "read":
        # Handle filtering by name if provided
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
                "Id": r[0],
                "FirstName": r[1],
                "LastName": r[2],
                "Name": r[3],
                "Email": r[4],
                "CreatedAt": r[5].isoformat()
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        # Initialize customer_name variable
        customer_name = None

        # Enhanced update: resolve customer_id from name if not provided
        if not customer_id and name:
            # Use the original find_customer_by_name function if enhanced version not available
            try:
                customer_info = find_customer_by_name(name)
                if not customer_info["found"]:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ {customer_info['error']}"}
                customer_id = customer_info["id"]
                customer_name = customer_info["name"]
            except Exception as search_error:
                # Fallback to direct database search
                cur.execute("""
                    SELECT Id, Name FROM Customers
                    WHERE LOWER(Name) = LOWER(%s)
                       OR LOWER(FirstName) = LOWER(%s)
                       OR LOWER(LastName) = LOWER(%s)
                    LIMIT 1
                """, (name, name, name))
                result = cur.fetchone()

                if result:
                    customer_id = result[0]
                    customer_name = result[1]
                else:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ Customer '{name}' not found"}

        if not customer_id or not new_email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' (or 'name') and 'new_email' required for update."}

        # Check if customer already has this email
        cur.execute("SELECT Name, Email FROM Customers WHERE Id = %s", (customer_id,))
        existing_customer = cur.fetchone()

        if not existing_customer:
            cnxn.close()
            return {"sql": None, "result": f"❌ Customer with ID {customer_id} not found."}

        # Set customer_name if not already set
        if not customer_name:
            customer_name = existing_customer[0]

        if existing_customer[1] == new_email:
            cnxn.close()
            return {"sql": None, "result": f"ℹ️ Customer '{customer_name}' already has email '{new_email}'."}

        sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
        cur.execute(sql_query, (new_email, customer_id))
        cnxn.commit()
        cnxn.close()

        return {"sql": sql_query, "result": f"✅ Customer '{customer_name}' email updated to '{new_email}'."}

    elif operation == "delete":
        # Initialize customer_name variable
        customer_name = None

        # Enhanced delete: resolve customer_id from name if not provided
        if not customer_id and name:
            try:
                customer_info = find_customer_by_name(name)
                if not customer_info["found"]:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ {customer_info['error']}"}
                customer_id = customer_info["id"]
                customer_name = customer_info["name"]
            except Exception as search_error:
                # Fallback to direct database search
                cur.execute("""
                    SELECT Id, Name FROM Customers
                    WHERE LOWER(Name) = LOWER(%s)
                       OR LOWER(FirstName) = LOWER(%s)
                       OR LOWER(LastName) = LOWER(%s)
                    LIMIT 1
                """, (name, name, name))
                result = cur.fetchone()

                if result:
                    customer_id = result[0]
                    customer_name = result[1]
                else:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ Customer '{name}' not found"}
        elif customer_id:
            # Get customer name for response
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
                "Field": r[0],
                "Type": r[1],
                "Null": r[2],
                "Key": r[3],
                "Default": r[4],
                "Extra": r[5]
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 8. Enhanced PostgreSQL CRUD Tool (Products) with Smart Name Resolution
# ————————————————
@mcp.tool()
async def postgresql_crud(
        operation: str,
        name: str = None,
        price: float = None,
        description: str = None,
        limit: int = 10,
        product_id: int = None,
        new_price: float = None,
        table_name: str = None,
) -> Any:
    """Manages product data in the PostgreSQL database. Use for creating, reading, updating, or deleting products."""
    cnxn = get_pg_conn()
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
        # Handle filtering by name if provided
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
        # Enhanced update: resolve product_id from name if not provided
        if not product_id and name:
            product_info = find_product_by_name(name)
            if not product_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {product_info['error']}"}
            product_id = product_info["id"]

        if not product_id or new_price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' (or 'name') and 'new_price' required for update."}

        sql_query = "UPDATE products SET price = %s WHERE id = %s"
        cur.execute(sql_query, (new_price, product_id))
        cnxn.commit()

        # Get updated product name for response
        cur.execute("SELECT name FROM products WHERE id = %s", (product_id,))
        product_name = cur.fetchone()
        product_name = product_name[0] if product_name else f"Product {product_id}"

        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Product '{product_name}' price updated to ${new_price:.2f}."}

    elif operation == "delete":
        # Enhanced delete: resolve product_id from name if not provided
        if not product_id and name:
            product_info = find_product_by_name(name)
            if not product_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {product_info['error']}"}
            product_id = product_info["id"]
            product_name = product_info["name"]
        elif product_id:
            # Get product name for response
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
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 9. Sales CRUD Tool with Display Formatting Features (Unchanged)
# ————————————————
# Fixed sales_crud function with proper column selection
# Fixed sales_crud function with proper WHERE clause and column selection
# Fixed sales_crud function with proper WHERE clause and column selection

@mcp.tool()
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
        where_clause: str = None,
        filter_conditions: dict = None,
        limit: int = None
) -> Any:
    # All operations (create, update, delete, read) now use MySQL
    """Manages sales data in the MySQL database. Use for creating, reading, updating, or deleting sales."""
    sales_cnxn = get_mysql_conn()
    sales_cur = sales_cnxn.cursor()

    if operation == "create":
        if not customer_id or not product_id:
            return {"sql": None, "result": "❌ 'customer_id' and 'product_id' required for create."}

        if not validate_customer_exists(customer_id):
            return {"sql": None, "result": f"❌ Customer ID {customer_id} not found."}

        if not validate_product_exists(product_id):
            return {"sql": None, "result": f"❌ Product ID {product_id} not found."}

        if not unit_price:
            product_details = get_product_details(product_id)
            unit_price = product_details["price"]

        if not total_amount:
            total_amount = unit_price * quantity
       # Resolve customer_id from customer_name if needed
        if not customer_id and customer_name:
            customer_id = get_customer_id_by_name(customer_name)
            if not customer_id:
                sales_cnxn.close()
                return {"sql": None, "result": f"❌ Customer with name '{customer_name}' not found."}

        # Resolve product_id from product_name if needed
        if not product_id and product_name:
            product_id = get_product_id_by_name(product_name)
            if not product_id:
                sales_cnxn.close()
                return {"sql": None, "result": f"❌ Product with name '{product_name}' not found."}


        sql_query = """
            INSERT INTO Sales (customer_id, product_id, quantity, unit_price, total_price)
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
            UPDATE Sales
            SET quantity = %s,
                total_price = unit_price * %s
            WHERE Id = %s
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

        sql_query = "DELETE FROM Sales WHERE Id = %s"
        sales_cur.execute(sql_query, (sale_id,))
        sales_cnxn.commit()
        result = f"✅ Sale id={sale_id} deleted."
        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    # Enhanced READ operation with FIXED column selection AND WHERE clause filtering
    elif operation == "read":
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()

        # Fixed column mappings - standardized naming
        available_columns = {
            "sale_id": "s.Id",
            "first_name": "c.FirstName",
            "last_name": "c.LastName",
            "customer_name": "c.Name",  # Use the Name field which has full name
            "product_name": "p.name",
            "product_description": "p.description",
            "quantity": "s.quantity",
            "unit_price": "s.unit_price",
            "total_price": "s.total_price",
            "amount": "s.total_price",  # Alias for total_price
            "sale_date": "s.sale_date",
            "date": "s.sale_date",  # Alias for sale_date
            "customer_email": "c.Email",
            "email": "c.Email"  # Alias for customer_email
        }

        # FIXED: Process column selection with better parsing
        selected_columns = []
        column_aliases = []

        print(f"DEBUG: Raw columns parameter: '{columns}'")

        if columns and columns.strip():
            # Clean and split the columns string
            columns_clean = columns.strip()

            # Handle different input patterns
            if "," in columns_clean:
                # Comma-separated list
                requested_cols = [col.strip().lower().replace(" ", "_") for col in columns_clean.split(",") if col.strip()]
            else:
                # Space-separated or single column
                requested_cols = [col.strip().lower().replace(" ", "_") for col in columns_clean.split() if col.strip()]

            print(f"DEBUG: Requested columns after parsing: {requested_cols}")

            # Build SELECT clause based on requested columns
            for col in requested_cols:
                matched = False
                # Try exact match first
                if col in available_columns:
                    selected_columns.append(available_columns[col])
                    column_aliases.append(col)
                    matched = True
                    print(f"DEBUG: Exact match found for '{col}': {available_columns[col]}")
                else:
                    # Try fuzzy matching for common variations
                    for avail_col, db_col in available_columns.items():
                        if (col in avail_col or avail_col in col or
                            col.replace("_", "") in avail_col.replace("_", "") or
                            avail_col.replace("_", "") in col.replace("_", "")):
                            selected_columns.append(db_col)
                            column_aliases.append(avail_col)
                            matched = True
                            print(f"DEBUG: Fuzzy match found for '{col}' -> '{avail_col}': {db_col}")
                            break

                if not matched:
                    print(f"DEBUG: No match found for column '{col}'. Skipping...")

        # If no valid columns found or no columns specified, use default key columns
        if not selected_columns:
            print("DEBUG: Using default key columns")
            selected_columns = [
                "s.Id", "c.Name", "p.name", "s.quantity", "s.unit_price", "s.total_price", "s.sale_date", "c.Email"
            ]
            column_aliases = [
                "sale_id", "customer_name", "product_name", "quantity", "unit_price", "total_price", "sale_date", "email"
            ]

        print(f"DEBUG: Final selected columns: {selected_columns}")
        print(f"DEBUG: Final column aliases: {column_aliases}")

        # Build dynamic SQL query
        select_clause = ", ".join([f"{col} AS {alias}" for col, alias in zip(selected_columns, column_aliases)])

        # Base query
        base_sql = f"""
        SELECT  {select_clause}
        FROM    Sales          s
        JOIN    Customers      c ON c.Id = s.customer_id
        JOIN    ProductsCache  p ON p.id = s.product_id
        """

        # COMPLETELY REWRITTEN WHERE clause processing
        where_sql = ""
        query_params = []

        if where_clause and where_clause.strip():
            print(f"DEBUG: Processing WHERE clause: '{where_clause}'")

            import re

            # Clean the input
            clause = where_clause.strip().lower()

            # Enhanced pattern matching for various query formats
            where_conditions = []

            # Pattern 1: "total_price > 50", "total price exceed 50", "total price exceeds $50"
            price_patterns = [
                r'total[_\s]*price[_\s]*(>|>=|exceed[s]?|above|greater\s+than|more\s+than)\s*\$?(\d+(?:\.\d+)?)',
                r'(>|>=|exceed[s]?|above|greater\s+than|more\s+than)\s*\$?(\d+(?:\.\d+)?)\s*total[_\s]*price',
                r'total[_\s]*price[_\s]*(<|<=|below|less\s+than|under)\s*\$?(\d+(?:\.\d+)?)',
                r'total[_\s]*price[_\s]*(=|equals?|is)\s*\$?(\d+(?:\.\d+)?)'
            ]

            for pattern in price_patterns:
                match = re.search(pattern, clause)
                if match:
                    if len(match.groups()) == 2:
                        operator_text, value = match.groups()
                        # Map operator text to SQL operator
                        if any(word in operator_text for word in ['exceed', 'above', 'greater', 'more', '>']):
                            operator = '>'
                        elif any(word in operator_text for word in ['below', 'less', 'under', '<']):
                            operator = '<'
                        elif any(word in operator_text for word in ['equal', 'is', '=']):
                            operator = '='
                        else:
                            operator = '>'  # default

                        where_conditions.append(f"s.total_price {operator} %s")
                        query_params.append(float(value))
                        print(f"DEBUG: Found price condition: s.total_price {operator} {value}")
                        break

            # Pattern 2: Quantity conditions
            quantity_patterns = [
                r'quantity[_\s]*(>|>=|greater\s+than|more\s+than|above)\s*(\d+)',
                r'quantity[_\s]*(<|<=|less\s+than|below|under)\s*(\d+)',
                r'quantity[_\s]*(=|equals?|is)\s*(\d+)'
            ]

            for pattern in quantity_patterns:
                match = re.search(pattern, clause)
                if match:
                    operator_text, value = match.groups()
                    if any(symbol in operator_text for symbol in ['>', 'greater', 'more', 'above']):
                        operator = '>'
                    elif any(symbol in operator_text for symbol in ['<', 'less', 'below', 'under']):
                        operator = '<'
                    else:
                        operator = '='

                    where_conditions.append(f"s.quantity {operator} %s")
                    query_params.append(int(value))
                    print(f"DEBUG: Found quantity condition: s.quantity {operator} {value}")
                    break

            # Pattern 3: Customer name conditions
            customer_patterns = [
                r'customer[_\s]*name[_\s]*like[_\s]*["\']([^"\']+)["\']',
                r'customer[_\s]*name[_\s]*=[_\s]*["\']([^"\']+)["\']',
                r'customer[_\s]*=[_\s]*["\']([^"\']+)["\']',
                r'customer[_\s]*name[_\s]*([a-zA-Z\s]+?)(?:\s|$)'
            ]

            for pattern in customer_patterns:
                match = re.search(pattern, clause)
                if match:
                    name_value = match.group(1).strip()
                    if 'like' in clause:
                        where_conditions.append("c.Name LIKE %s")
                        query_params.append(f"%{name_value}%")
                    else:
                        where_conditions.append("c.Name = %s")
                        query_params.append(name_value)
                    print(f"DEBUG: Found customer condition: {name_value}")
                    break

            # Pattern 4: Product name conditions
            product_patterns = [
                r'product[_\s]*name[_\s]*like[_\s]*["\']([^"\']+)["\']',
                r'product[_\s]*name[_\s]*=[_\s]*["\']([^"\']+)["\']',
                r'product[_\s]*=[_\s]*["\']([^"\']+)["\']'
            ]

            for pattern in product_patterns:
                match = re.search(pattern, clause)
                if match:
                    product_value = match.group(1).strip()
                    if 'like' in clause:
                        where_conditions.append("p.name LIKE %s")
                        query_params.append(f"%{product_value}%")
                    else:
                        where_conditions.append("p.name = %s")
                        query_params.append(product_value)
                    print(f"DEBUG: Found product condition: {product_value}")
                    break

            # If no specific patterns matched, try a generic approach
            if not where_conditions:
                # Look for any number that might be a price threshold
                number_match = re.search(r'\$?(\d+(?:\.\d+)?)', clause)
                if number_match:
                    value = float(number_match.group(1))
                    # Default to total_price filter if no specific field mentioned
                    if any(word in clause for word in ['exceed', 'above', 'greater', 'more']):
                        where_conditions.append("s.total_price > %s")
                    elif any(word in clause for word in ['below', 'less', 'under']):
                        where_conditions.append("s.total_price < %s")
                    else:
                        where_conditions.append("s.total_price > %s")  # Default assumption

                    query_params.append(value)
                    print(f"DEBUG: Generic number condition: {value}")

            # Build the WHERE clause
            if where_conditions:
                where_sql = " WHERE " + " AND ".join(where_conditions)
                print(f"DEBUG: Final WHERE clause: {where_sql}")
                print(f"DEBUG: Query parameters: {query_params}")

        # Handle structured filter conditions (alternative to where_clause)
        elif filter_conditions:
            where_conditions = []
            for field, value in filter_conditions.items():
                if field in available_columns:
                    db_field = available_columns[field]
                    if isinstance(value, str):
                        where_conditions.append(f"{db_field} LIKE %s")
                        query_params.append(f"%{value}%")
                    else:
                        where_conditions.append(f"{db_field} = %s")
                        query_params.append(value)

            if where_conditions:
                where_sql = " WHERE " + " AND ".join(where_conditions)

        # Add ORDER BY and LIMIT
        order_sql = " ORDER BY s.sale_date DESC"
        limit_sql = ""
        if limit:
            limit_sql = f" LIMIT {limit}"

        # Complete SQL query
        sql = base_sql + where_sql + order_sql + limit_sql

        print(f"DEBUG: Final SQL: {sql}")
        print(f"DEBUG: Final Parameters: {query_params}")

        # Execute query
        try:
            if query_params:
                mysql_cur.execute(sql, query_params)
            else:
                mysql_cur.execute(sql)

            rows = mysql_cur.fetchall()
            print(f"DEBUG: Query returned {len(rows)} rows")
        except Exception as e:
            mysql_cnxn.close()
            return {"sql": sql, "result": f"❌ SQL Error: {str(e)}"}

        mysql_cnxn.close()

        # Build result with only requested columns
        processed_results = []
        for r in rows:
            row_data = {}
            for i, alias in enumerate(column_aliases):
                if i < len(r):  # Safety check
                    value = r[i]

                    # Apply formatting based on display_format
                    if display_format == "Data Format Conversion":
                        if "date" in alias or "timestamp" in alias:
                            value = value.strftime("%Y-%m-%d %H:%M:%S") if value else "N/A"
                    elif display_format == "Decimal Value Formatting":
                        if "price" in alias or "total" in alias or "amount" in alias:
                            value = f"{float(value):.2f}" if value is not None else "0.00"
                    elif display_format == "Null Value Removal/Handling":
                        if value is None:
                            value = "N/A"

                    row_data[alias] = value

            # Handle String Concatenation for specific display format
            if display_format == "String Concatenation":
                if "customer_name" in row_data or ("first_name" in row_data and "last_name" in row_data):
                    if "first_name" in row_data and "last_name" in row_data:
                        row_data["customer_full_name"] = f"{row_data['first_name']} {row_data['last_name']}"

                if "product_name" in row_data and "product_description" in row_data:
                    desc = row_data['product_description'] or 'No description'
                    row_data["product_full_description"] = f"{row_data['product_name']} ({desc})"

                # Create sale summary if we have the needed fields
                if all(field in row_data for field in ['customer_name', 'quantity', 'product_name', 'total_price']):
                    row_data["sale_summary"] = (
                        f"{row_data['customer_name']} bought {row_data['quantity']} "
                        f"of {row_data['product_name']} for ${float(row_data['total_price']):.2f}"
                    )

            # Skip null records if specified
            if display_format == "Null Value Removal/Handling":
                if any(v is None for v in row_data.values()):
                    continue

            processed_results.append(row_data)

        print(f"DEBUG: Processed results count: {len(processed_results)}")
        if processed_results:
            print(f"DEBUG: First result keys: {list(processed_results[0].keys())}")

        return {"sql": sql, "result": processed_results}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 10. Enhanced CarePlan Tool with Comprehensive Fields
# ————————————————
@mcp.tool()
async def careplan_crud(
        operation: str,
        columns: str = None,
        where_clause: str = None,
        limit: int = None,
        care_plan_type: str = None,
        status: str = None
) -> Any:
    if operation != "read":
        return {"sql": None, "result": "❌ Only 'read' operation is supported for care plans."}

    conn = get_mysql_conn()
    cur = conn.cursor()

    # Mapping for clean column naming with all the new comprehensive fields
    available_columns = {
        # Base Information
        "id": "ID",
        "actual_release_date": "ActualReleaseDate",
        "name_of_youth": "NameOfYouth",
        "race_ethnicity": "RaceEthnicity",
        "medi_cal_id_number": "MediCalIDNumber",
        "residential_address": "ResidentialAddress",
        "telephone": "Telephone",
        "medi_cal_health_plan": "MediCalHealthPlan",

        # Health Information
        "health_screenings": "HealthScreenings",
        "health_assessments": "HealthAssessments",
        "chronic_conditions": "ChronicConditions",
        "prescribed_medications": "PrescribedMedications",

        # Reentry Specific Fields
        "screenings": "Screenings",
        "clinical_assessments": "ClinicalAssessments",
        "treatment_history": "TreatmentHistory",
        "primary_physician_contacts": "PrimaryPhysicianContacts",
        "scheduled_appointments": "ScheduledAppointments",
        "housing": "Housing",
        "employment": "Employment",
        "income_benefits": "IncomeBenefits",
        "food_clothing": "FoodClothing",
        "transportation": "Transportation",
        "identification_documents": "IdentificationDocuments",
        "life_skills": "LifeSkills",
        "family_children": "FamilyChildren",
        "emergency_contacts": "EmergencyContacts",
        "court_dates": "CourtDates",
        "service_referrals": "ServiceReferrals",
        "home_modifications": "HomeModifications",
        "durable_medical_equipment": "DurableMedicalEquipment",

        # Metadata
        "care_plan_type": "CarePlanType",
        "created_at": "CreatedAt",
        "updated_at": "UpdatedAt",
        "status": "Status",
        "notes": "Notes"
    }

    selected_columns = []
    column_aliases = []

    if columns and columns.strip():
        raw_cols = columns.strip().lower()
        if raw_cols.startswith("*"):
            selected_columns = list(available_columns.values())
            column_aliases = list(available_columns.keys())

            # Remove exclusions like *,-address,-phone
            exclusions = [col.strip().replace("-", "").replace(" ", "_")
                          for col in raw_cols.split(",") if col.startswith("-")]
            selected_columns, column_aliases = zip(*[
                (col_db, col_alias)
                for col_alias, col_db in available_columns.items()
                if col_alias not in exclusions
            ])
        else:
            requested = [c.strip().lower().replace(" ", "_") for c in raw_cols.split(",")]
            for col in requested:
                if col in available_columns:
                    selected_columns.append(available_columns[col])
                    column_aliases.append(col)
                else:
                    # Try fuzzy matching for common variations
                    for avail_col, db_col in available_columns.items():
                        if (col in avail_col or avail_col in col or
                            col.replace("_", "") in avail_col.replace("_", "") or
                            avail_col.replace("_", "") in col.replace("_", "")):
                            selected_columns.append(db_col)
                            column_aliases.append(avail_col)
                            break
    else:
        # Default to key columns
        selected_columns = [
            "ID", "NameOfYouth", "RaceEthnicity", "CarePlanType", "Status",
            "ChronicConditions", "Housing", "Employment"
        ]
        column_aliases = [
            "id", "name_of_youth", "race_ethnicity", "care_plan_type", "status",
            "chronic_conditions", "housing", "employment"
        ]

    select_clause = ", ".join([f"{db_col} AS {alias}" for db_col, alias in zip(selected_columns, column_aliases)])
    sql = f"SELECT {select_clause} FROM CarePlan WHERE 1=1"
    query_params = []

    # Add filters
    if care_plan_type:
        sql += " AND CarePlanType = %s"
        query_params.append(care_plan_type)

    if status:
        sql += " AND Status = %s"
        query_params.append(status)

    if where_clause and where_clause.strip():
        sql += f" AND {where_clause}"

    if limit:
        sql += f" LIMIT {limit}"

    try:
        cur.execute(sql, query_params)
        rows = cur.fetchall()
    except Exception as e:
        conn.close()
        return {"sql": sql, "result": f"❌ SQL Error: {str(e)}"}

    conn.close()

    # Process results with proper type handling
    results = []
    for row in rows:
        row_dict = {}
        for i, alias in enumerate(column_aliases):
            if i < len(row):
                value = row[i]
                # Handle date serialization
                if alias in ["actual_release_date", "created_at", "updated_at"] and value:
                    value = value.isoformat()
                row_dict[alias] = value
        results.append(row_dict)

    return {"sql": sql, "result": results}



# -----------------------
# CallLogs CRUD TOOL
# -----------------------
@mcp.tool()
async def calllogs_crud(
        operation: str,
        analysis_type: str = None,
        date_range: str = None,
        agent_name: str = None,
        issue_category: str = None,
        sentiment_threshold: float = None,
        columns: str = None,  # ADD THIS PARAMETER
        where_clause: str = None,  # ADD THIS FOR FILTERING
        limit: int = 50
) -> Any:
    """Analyzes call log data for customer service insights. Supports various analysis types including sentiment analysis, agent performance, issue trends, and call patterns."""

    conn = get_mysql_conn()
    cur = conn.cursor()

    if operation == "read":
        # Define available columns for call logs
        available_columns = {
            "log_id": "cl.LogID",
            "call_date": "cl.CallDate",
            "customer_id": "cl.CustomerID",
            "customer_name": "c.Name",
            "agent_name": "cl.AgentName",
            "call_duration": "cl.CallDuration",
            "call_type": "cl.CallType",
            "call_status": "cl.CallStatus",
            "issue_category": "cl.IssueCategory",
            "resolution_status": "cl.ResolutionStatus",
            "sentiment_score": "cl.SentimentScore",
            "call_notes": "cl.CallNotes",
            "wait_time": "cl.WaitTime",
            "transfer_count": "cl.TransferCount"
        }

        # Process column selection
        selected_columns = []
        column_aliases = []

        if columns and columns.strip():
            columns_clean = columns.strip().lower()

            if columns_clean.startswith("*"):
                # Select all columns
                selected_columns = list(available_columns.values())
                column_aliases = list(available_columns.keys())

                # Handle exclusions like *,-call_notes
                exclusions = [col.strip().replace("-", "").replace(" ", "_").lower()
                            for col in columns_clean.split(",") if col.startswith("-")]

                if exclusions:
                    filtered = [(alias, col) for alias, col in available_columns.items()
                               if alias not in exclusions]
                    if filtered:
                        column_aliases, selected_columns = zip(*filtered)
                        column_aliases = list(column_aliases)
                        selected_columns = list(selected_columns)
            else:
                # Process specific columns
                requested_cols = [col.strip().lower().replace(" ", "_")
                                for col in columns_clean.split(",") if col.strip()]

                for col in requested_cols:
                    if col in available_columns:
                        selected_columns.append(available_columns[col])
                        column_aliases.append(col)
                    else:
                        # Try fuzzy matching
                        for avail_col, db_col in available_columns.items():
                            if (col in avail_col or avail_col in col or
                                col.replace("_", "") in avail_col.replace("_", "")):
                                selected_columns.append(db_col)
                                column_aliases.append(avail_col)
                                break

        # Default columns if none specified
        if not selected_columns:
            selected_columns = [
                "cl.LogID", "cl.CallDate", "c.Name", "cl.AgentName",
                "cl.CallDuration", "cl.CallType", "cl.IssueCategory",
                "cl.ResolutionStatus", "cl.SentimentScore"
            ]
            column_aliases = [
                "log_id", "call_date", "customer_name", "agent_name",
                "call_duration", "call_type", "issue_category",
                "resolution_status", "sentiment_score"
            ]

        # Build SELECT clause
        select_clause = ", ".join([f"{col} AS {alias}"
                                  for col, alias in zip(selected_columns, column_aliases)])

        sql = f"""
            SELECT {select_clause}
            FROM CallLogs cl
            LEFT JOIN Customers c ON cl.CustomerID = c.Id
            WHERE 1=1
        """
        params = []

        # Add filter conditions
        if agent_name:
            sql += " AND cl.AgentName = %s"
            params.append(agent_name)

        if issue_category:
            sql += " AND cl.IssueCategory = %s"
            params.append(issue_category)

        if sentiment_threshold is not None:
            sql += " AND cl.SentimentScore >= %s"
            params.append(sentiment_threshold)

        # Add custom WHERE clause if provided
        if where_clause and where_clause.strip():
            sql += f" AND {where_clause}"

        sql += " ORDER BY cl.CallDate DESC LIMIT %s"
        params.append(limit)

        cur.execute(sql, params)
        rows = cur.fetchall()

        result = []
        for r in rows:
            row_dict = {}
            for i, alias in enumerate(column_aliases):
                if i < len(r):
                    value = r[i]
                    # Handle datetime serialization
                    if alias == "call_date" and value:
                        value = value.isoformat()
                    # Handle decimal serialization
                    elif alias == "sentiment_score" and value is not None:
                        value = float(value)
                    row_dict[alias] = value
            result.append(row_dict)

        conn.close()
        return {"sql": sql, "result": result}

    elif operation == "analyze":
        # Keep all the existing analysis types exactly as they were
        if analysis_type == "sentiment_by_agent":
            sql = """
                SELECT AgentName,
                       AVG(SentimentScore) as AvgSentiment,
                       COUNT(*) as TotalCalls,
                       SUM(CASE WHEN SentimentScore >= 0.5 THEN 1 ELSE 0 END) as PositiveCalls
                FROM CallLogs
                GROUP BY AgentName
                ORDER BY AvgSentiment DESC
            """
            cur.execute(sql)
            rows = cur.fetchall()
            result = [{"AgentName": r[0], "AvgSentiment": float(r[1]),
                      "TotalCalls": r[2], "PositiveCalls": r[3]} for r in rows]

        elif analysis_type == "issue_frequency":
            sql = """
                SELECT IssueCategory,
                       COUNT(*) as Frequency,
                       AVG(CallDuration) as AvgDuration,
                       SUM(CASE WHEN ResolutionStatus = 'resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as ResolutionRate
                FROM CallLogs
                GROUP BY IssueCategory
                ORDER BY Frequency DESC
            """
            cur.execute(sql)
            rows = cur.fetchall()
            result = [{"IssueCategory": r[0], "Frequency": r[1],
                      "AvgDuration": r[2], "ResolutionRate": float(r[3])} for r in rows]

        elif analysis_type == "call_volume_trends":
            sql = """
                SELECT DATE(CallDate) as Date,
                       COUNT(*) as CallCount,
                       AVG(WaitTime) as AvgWaitTime,
                       AVG(CallDuration) as AvgDuration
                FROM CallLogs
                GROUP BY DATE(CallDate)
                ORDER BY Date DESC
                LIMIT 30
            """
            cur.execute(sql)
            rows = cur.fetchall()
            result = [{"Date": r[0].isoformat(), "CallCount": r[1],
                      "AvgWaitTime": r[2], "AvgDuration": r[3]} for r in rows]

        elif analysis_type == "escalation_analysis":
            sql = """
                SELECT IssueCategory,
                       COUNT(*) as TotalCalls,
                       SUM(CASE WHEN ResolutionStatus = 'escalated' THEN 1 ELSE 0 END) as EscalatedCalls,
                       SUM(CASE WHEN ResolutionStatus = 'escalated' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as EscalationRate
                FROM CallLogs
                GROUP BY IssueCategory
                HAVING EscalationRate > 0
                ORDER BY EscalationRate DESC
            """
            cur.execute(sql)
            rows = cur.fetchall()
            result = [{"IssueCategory": r[0], "TotalCalls": r[1],
                      "EscalatedCalls": r[2], "EscalationRate": float(r[3])} for r in rows]

        elif analysis_type == "agent_performance":
            sql = """
                SELECT AgentName,
                       COUNT(*) as TotalCalls,
                       AVG(CallDuration) as AvgCallDuration,
                       AVG(SentimentScore) as AvgSentiment,
                       SUM(CASE WHEN ResolutionStatus = 'resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as ResolutionRate,
                       AVG(TransferCount) as AvgTransfers
                FROM CallLogs
                GROUP BY AgentName
                ORDER BY ResolutionRate DESC
            """
            cur.execute(sql)
            rows = cur.fetchall()
            result = [{"AgentName": r[0], "TotalCalls": r[1],
                      "AvgCallDuration": r[2], "AvgSentiment": float(r[3]),
                      "ResolutionRate": float(r[4]), "AvgTransfers": float(r[5])} for r in rows]
        else:
            result = "Unknown analysis type. Available types: sentiment_by_agent, issue_frequency, call_volume_trends, escalation_analysis, agent_performance"

        conn.close()
        return {"sql": sql if analysis_type != None else None, "result": result}

    else:
        conn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 11. Main: seed + run server
# ————————————————
if __name__ == "__main__":
    # 1) Create + seed all databases (if needed)
    seed_databases()

    # 2) Launch the MCP server for cloud deployment
    import os

    port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
