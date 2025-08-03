    import os, re, json, ast, asyncio
import pandas as pd
import streamlit as st
import base64
from io import BytesIO
from PIL import Image
from groq import Groq 
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
import streamlit.components.v1 as components
from datetime import datetime, date # Import datetime and date for date formatting
from decimal import Decimal, InvalidOperation # Import Decimal for precise decimal handling

from dotenv import load_dotenv

load_dotenv()
# ========== PAGE CONFIG ==========
st.set_page_config(page_title="MCP CRUD Chat", layout="wide")

# ========== GLOBAL CSS ==========
st.markdown("""
    <style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #4286f4 0%, #397dd2 100%);
        color: #fff !important;
        min-width: 330px !important;
        padding: 0 0 0 0 !important;
    }
    [data-testid="stSidebar"] .sidebar-title {
        color: #fff !important;
        font-weight: bold;
        font-size: 2.2rem;
        letter-spacing: -1px;
        text-align: center;
        margin-top: 28px;
        margin-bottom: 18px;
    }
    .sidebar-block {
        width: 94%;
        margin: 0 auto 18px auto;
    }
    .sidebar-block label {
        color: #fff !important;
        font-weight: 500;
        font-size: 1.07rem;
        margin-bottom: 4px;
        margin-left: 2px;
        display: block;
        text-align: left;
    }
    .sidebar-block .stSelectbox>div {
        background: #fff !important;
        color: #222 !important;
        border-radius: 13px !important;
        font-size: 1.13rem !important;
        min-height: 49px !important;
        box-shadow: 0 3px 14px #0002 !important;
        padding: 3px 10px !important;
        margin-top: 4px !important;
        margin-bottom: 0 !important;
    }
    .stButton>button {
            width: 100%;
            height: 3rem;
            background: #39e639;
            color: #222;
            font-size: 1.1rem;
            font-weight: bold;
            border-radius: 10px;
            margin-bottom: 2rem;
        }
    /* Small refresh button styling */
    .small-refresh-button button {
        width: auto !important;
        height: 2rem !important;
        background: #4286f4 !important;
        color: #fff !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        border-radius: 6px !important;
        margin-bottom: 0.5rem !important;
        padding: 0.25rem 0.75rem !important;
        border: none !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    .small-refresh-button button:hover {
        background: #397dd2 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
    }
    .sidebar-logo-label {
        margin-top: 30px !important;
        margin-bottom: 10px;
        font-size: 1.13rem !important;
        font-weight: 600;
        text-align: center;
        color: #fff !important;
        letter-spacing: 0.1px;
    }
    .sidebar-logo-row {
        display: flex;
        flex-direction: row;
        justify-content: center;
        align-items: center;
        gap: 20px;
        margin-top: 8px;
        margin-bottom: 8px;
    }
    .sidebar-logo-row img {
        width: 42px;
        height: 42px;
        border-radius: 9px;
        background: #fff;
        padding: 6px 8px;
        object-fit: contain;
        box-shadow: 0 2px 8px #0002;
    }
    /* Chat area needs bottom padding so sticky bar does not overlap */
    .stChatPaddingBottom { padding-bottom: 98px; }
    /* Responsive sticky chatbar */
    .sticky-chatbar {
        position: fixed;
        left: 330px;
        right: 0;
        bottom: 0;
        z-index: 100;
        background: #f8fafc;
        padding: 0.6rem 2rem 0.8rem 2rem;
        box-shadow: 0 -2px 24px #0001;
    }
    @media (max-width: 800px) {
        .sticky-chatbar { left: 0; right: 0; padding: 0.6rem 0.5rem 0.8rem 0.5rem; }
        [data-testid="stSidebar"] { display: none !important; }
    }
    .chat-bubble {
        padding: 13px 20px;
        margin: 8px 0;
        border-radius: 18px;
        max-width: 75%;
        font-size: 1.09rem;
        line-height: 1.45;
        box-shadow: 0 1px 4px #0001;
        display: inline-block;
        word-break: break-word;
    }
    .user-msg {
        background: #e6f0ff;
        color: #222;
        margin-left: 24%;
        text-align: right;
        border-bottom-right-radius: 6px;
        border-top-right-radius: 24px;
    }
    .agent-msg {
        background: #f5f5f5;
        color: #222;
        margin-right: 24%;
        text-align: left;
        border-bottom-left-radius: 6px;
        border-top-left-radius: 24px;
    }
    .chat-row {
        display: flex;
        align-items: flex-end;
        margin-bottom: 0.6rem;
    }
    .avatar {
        height: 36px;
        width: 36px;
        border-radius: 50%;
        margin: 0 8px;
        object-fit: cover;
        box-shadow: 0 1px 4px #0002;
    }
    .user-avatar { order: 2; }
    .agent-avatar { order: 0; }
    .user-bubble { order: 1; }
    .agent-bubble { order: 1; }
    .right { justify-content: flex-end; }
    .left { justify-content: flex-start; }
    .chatbar-claude {
        display: flex;
        align-items: center;
        gap: 12px;
        width: 100%;
        max-width: 850px;
        margin: 0 auto;
        border-radius: 20px;
        background: #fff;
        box-shadow: 0 2px 8px #0002;
        padding: 8px 14px;
        margin-bottom: 0;
    }
    .claude-hamburger {
        background: #f2f4f9;
        border: none;
        border-radius: 11px;
        font-size: 1.35rem;
        font-weight: bold;
        width: 38px; height: 38px;
        cursor: pointer;
        display: flex; align-items: center; justify-content: center;
        transition: background 0.13s;
    }
    .claude-hamburger:hover { background: #e6f0ff; }
    .claude-input {
        flex: 1;
        border: none;
        outline: none;
        font-size: 1.12rem;
        padding: 0.45rem 0.5rem;
        background: #f5f7fa;
        border-radius: 8px;
        min-width: 60px;
    }
    .claude-send {
        background: #fe3044 !important;
        color: #fff !important;
        border: none;
        border-radius: 50%;
        width: 40px; height: 40px;
        font-size: 1.4rem !important;
        cursor: pointer;
        display: flex; align-items: center; justify-content: center;
        transition: background 0.17s;
    }
    .claude-send:hover { background: #d91d32 !important; }
    .tool-menu {
        position: fixed;
        top: 20px;
        right: 20px;
        background: #fff;
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        z-index: 1000;
        min-width: 200px;
    }
    .server-title {
        font-weight: bold;
        margin-bottom: 10px;
        color: #333;
    }
    .expandable {
        margin-top: 8px;
    }

    [data-testid="stSidebar"] .stSelectbox label {
        color: #fff !important;
        font-weight: 500;
        font-size: 1.07rem;
    }
    </style>
""", unsafe_allow_html=True)


# ========== DYNAMIC TOOL DISCOVERY FUNCTIONS ==========
async def _discover_tools() -> dict:
    """Discover available tools from the MCP server"""
    try:
        transport = StreamableHttpTransport(f"{st.session_state.get('MCP_SERVER_URL', 'http://localhost:8000/mcp')}") 
        async with Client(transport) as client:
            tools = await client.list_tools()
            return {tool.name: tool.description for tool in tools}
    except Exception as e:
        st.error(f"Failed to discover tools: {e}")
        return {}


def discover_tools() -> dict:
    """Synchronous wrapper for tool discovery"""
    return asyncio.run(_discover_tools())


def generate_tool_descriptions(tools_dict: dict) -> str:
    """Generate tool descriptions string from discovered tools"""
    if not tools_dict:
        return "No tools available"

    descriptions = ["Available tools:"]
    for i, (tool_name, tool_desc) in enumerate(tools_dict.items(), 1):
        descriptions.append(f"{i}. {tool_name}: {tool_desc}")

    return "\n".join(descriptions)


# ========== SIDEBAR NAVIGATION ==========
with st.sidebar:
    st.markdown("<div class='sidebar-title'>Solutions Scope</div>", unsafe_allow_html=True)
    with st.container():
        # Application selectbox (with key)
        application = st.selectbox(
            "Select Application",
            ["Select Application", "MCP Application"],
            key="app_select"
        )

        # Dynamically choose default options for other selects
        # Option lists
        protocol_options = ["", "MCP Protocol", "A2A Protocol"]
        # MODIFIED: Added Groq models
        llm_options = ["", "GPT-4o", "GPT-4", "Claude 3 Sonnet", "Claude 3 Opus", "Groq - Llama3-8b-8192", "Groq - Llama3-70b-8192", "Groq - Mixtral-8x7b-32768"]

        # Logic to auto-select defaults if MCP Application is chosen
        protocol_index = protocol_options.index(
            "MCP Protocol") if application == "MCP Application" else protocol_options.index(
            st.session_state.get("protocol_select", ""))
        llm_index = llm_options.index("Groq - Llama3-8b-8192") if application == "MCP Application" else llm_options.index( # Default to Groq
            st.session_state.get("llm_select", ""))

        protocol = st.selectbox(
            "Protocol",
            protocol_options,
            key="protocol_select",
            index=protocol_index
        )

        llm_model = st.selectbox(
            "LLM Models",
            llm_options,
            key="llm_select",
            index=llm_index
        )

        # Dynamic server tools selection based on discovered tools
        if application == "MCP Application" and "available_tools" in st.session_state and st.session_state.available_tools:
            server_tools_options = [""] + list(st.session_state.available_tools.keys())
            default_tool = list(st.session_state.available_tools.keys())[0] if st.session_state.available_tools else ""
            server_tools_index = server_tools_options.index(default_tool) if default_tool else 0
        else:
            server_tools_options = ["", "sqlserver_crud", "postgresql_crud", "sales_crud"]  # Fallback
            server_tools_index = 0

        server_tools = st.selectbox(
            "Server Tools",
            server_tools_options,
            key="server_tools",
            index=server_tools_index
        )

        # NEW: Dropdown for display options
        display_options = [
            "Default Formatting",
            "Data Format Conversion",
            "Decimal Value Formatting",
            "String Concatenation",
            "Null Value Removal/Handling"
        ]
        selected_display_option = st.selectbox(
            "Display Options",
            display_options,
            key="display_option_select"
        )

        st.button("Clear/Reset", key="clear_button")

    st.markdown('<div class="sidebar-logo-label">Build & Deployed on</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="sidebar-logo-row">
            <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/googlecloud/googlecloud-original.svg" title="Google Cloud">
            <img src="https://a0.awsstatic.com/libra-css/images/logos/aws_logo_smile_1200x630.png" title="AWS">
            <img src="https://upload.wikimedia.org/wikipedia/commons/a/a8/Microsoft_Azure_Logo.svg" title="Azure Cloud">
        </div>
        """,
        unsafe_allow_html=True
    )


# ========== LOGO/HEADER FOR MAIN AREA ==========
def get_image_base64(img_path):
    img = Image.open(img_path)
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    img_bytes = buffered.getvalue()
    img_base64 = base64.b64encode(img_bytes).decode()
    return img_base64


logo_path = "Logo.png"
logo_base64 = get_image_base64(logo_path) if os.path.exists(logo_path) else ""
if logo_base64:
    st.markdown(
        f"""
        <div style='display: flex; flex-direction: column; align-items: center; margin-bottom:20px;'>
            <img src='data:image/png;base64,{logo_base64}' width='220'>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown(
    """
    <div style="
        display: flex;
        flex-direction: column;
        align-items: center;
        margin-bottom: 18px;
        padding: 10px 0 10px 0;
    ">
        <span style="
            font-size: 2.5rem;
            font-weight: bold;
            letter-spacing: -2px;
            color: #222;
        ">
            MCP-Driven Data Management Implementation
        </span>
        <span style="
            font-size: 1.15rem;
            color: #555;
            margin-top: 0.35rem;
        ">
            Agentic Platform: Leveraging MCP and LLMs for Secure CRUD Operations and Instant Analytics on SQL Server and PostgreSQL.
        </span>
        <hr style="
        width: 80%;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #4286f4, transparent);
        margin: 20px auto;
        ">
    </div>

    """,
    unsafe_allow_html=True
)

# ========== SESSION STATE INIT ==========
if "messages" not in st.session_state:
    st.session_state.messages = []

# Initialize available_tools if not exists
if "available_tools" not in st.session_state:
    st.session_state.available_tools = {}

# Initialize MCP_SERVER_URL in session state
if "MCP_SERVER_URL" not in st.session_state:
    st.session_state["MCP_SERVER_URL"] = os.getenv("MCP_SERVER_URL", "http://localhost:8000/mcp") 

# ADD THIS NEW LINE:
st.sidebar.info(f"Client trying to connect to: {st.session_state['MCP_SERVER_URL']}")


# Initialize tool_states dynamically based on discovered tools
if "tool_states" not in st.session_state:
    st.session_state.tool_states = {}

if "show_menu" not in st.session_state:
    st.session_state["show_menu"] = False
if "menu_expanded" not in st.session_state:
    st.session_state["menu_expanded"] = True
if "chat_input_box" not in st.session_state:
    st.session_state["chat_input_box"] = ""

# NEW: State for conversational loop
if "pending_tool_call" not in st.session_state:
    st.session_state.pending_tool_call = None
if "awaiting_input_for_field" not in st.session_state:
    st.session_state.awaiting_input_for_field = None


# ========== HELPER FUNCTIONS ==========
def _clean_json(raw: str) -> str:
    fences = re.findall(r"```", raw, re.DOTALL)
    return fences[0].strip() if fences else raw.strip()


# ========== NEW LLM RESPONSE GENERATOR ==========
def generate_llm_response(operation_result: dict, action: str, tool: str, user_query: str, llm_model: str) -> str:
    """Generate LLM response based on operation result with context"""

    # Prepare context for LLM
    context = {
        "action": action,
        "tool": tool,
        "user_query": user_query,
        "operation_result": operation_result
    }

    system_prompt = (
            "You are an intelligent sales agent and database router for CRUD operations. "
    "Your job is to analyze the user's query and select the most appropriate tool based on the tool descriptions provided.\n\n"

    "AS A SALES AGENT, YOU SHOULD:\n"
    "- Understand business context and customer needs\n"
    "- Recognize sales-related queries (orders, transactions, revenue, customer purchases)\n"
    "- Identify cross-database relationships (customer orders, product sales, inventory)\n"
    "- Provide intelligent routing for business analytics and reporting needs\n"
    "- Handle complex queries that may involve multiple data sources\n\n"

    "RESPONSE FORMAT:\n"
    "Reply with exactly one JSON object: {\"tool\": string, \"action\": string, \"args\": object}\n\n"

    "ACTION MAPPING:\n"
    "- 'read': for viewing, listing, showing, displaying, or getting records\n"
    "- 'create': for adding, inserting, or creating new records (orders, customers, products)\n"
    "- 'update': for modifying, changing, or updating existing records\n"
    "- 'delete': for removing, deleting, or destroying records\n"
    "- 'describe': for showing table structure, schema, or column information\n\n"

    "TOOL SELECTION GUIDELINES:\n"
    "- Use `sales_crud` for any of the following:\n"
    "  * Sales, transactions, orders, customer purchases, product sales, revenue\n"
    "  * Phrases like 'record a sale', 'customer buys product', 'show sales'\n"
    "  * Queries where both customers and products are mentioned but the focus is a transaction\n"
    "- Use `sqlserver_crud` only for:\n"
    "  * Customer-related queries (adding/updating/listing customer records)\n"
    "  * Phrases like 'add customer', 'update email', 'list customers'\n"
    "- Use `postgresql_crud` only for:\n"
    "  * Product management (name, price, description, category, launch date)\n"
    "  * Phrases like 'add product', 'update product', 'product catalog'\n\n"

    "SALES-SPECIFIC ROUTING EXAMPLES:\n"
    "- 'record sale for customer John buying 2 of product Widget' → `sales_crud`\n"
    "- 'list all sales where quantity >= 3' → `sales_crud`\n"
    "- 'create order for product X and customer Y' → `sales_crud`\n"
    "- 'show all transactions this month' → `sales_crud`\n"
    "- 'add customer John Doe' → `sqlserver_crud`\n"
    "- 'show customer list' → `sqlserver_crud`\n"
    "- 'add product iPhone for 1200.99' → `postgresql_crud`\n"
    "- 'update product 5 price to 299.99' → `postgresql_crud`\n\n"

    "ARGUMENT EXTRACTION:\n"
    "- `sales_crud`: use `customer_id`, `product_id`, `quantity`, `unit_price`, `total_amount`, `sale_id`, `new_quantity`, or use `customer_name` and `product_name` if IDs are not available\n"
    "- `sqlserver_crud`: use `first_name`, `last_name`, `email`, `customer_id`, `new_email`, `columns`, `where_clause`\n"
    "- `postgresql_crud`: use `name`, `price`, `description`, `product_id`, `category`, `launch_date`, `new_price`, `new_quantity`, `columns`, `where_clause`\n\n"


    "ETL GUIDANCE FOR LLM:\n"
    "- Convert date formats like '31st July 2025' to '2025-07-31'\n"
    "- Extract numeric values like price as float\n"
    "- Split full names into first and last name if required\n"
    "- If category is not given, let the server default to 'Uncategorized'\n\n"

    "If in doubt, route to `sales_crud` when transactions or purchases are involved."
    )

    user_prompt = f"""
    Based on this database operation context, generate a brief natural response:

    User asked: "{user_query}"
    Operation: {action}
    Tool used: {tool}
    Result: {json.dumps(operation_result, indent=2)}

    Generate a single line response explaining what was done and the outcome.
    """

    try:
        if llm_model.startswith("Groq"):
            groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))
            model_name = llm_model.split(" - ")[1] # Extract model name like "llama3-8b-8192"
            response = groq_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=100
            )
        else: # Fallback to OpenAI if not Groq (or other future LLMs)
            openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            response = openai_client.chat.completions.create(
                model=llm_model.lower().replace(" ", "-"), # Adjust model name for OpenAI
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=100
            )
        return response.choices[0].message.content.strip()
    except Exception as e:
        # Fallback response if LLM call fails
        st.error(f"LLM Response Generation Error: {e}") # Display error for debugging
        if action == "read":
            return f"Successfully retrieved data from {tool}."
        elif action == "create":
            return f"Successfully created new record in {tool}."
        elif action == "update":
            return f"Successfully updated record in {tool}."
        elif action == "delete":
            return f"Successfully deleted record from {tool}."
        elif action == "describe":
            return f"Successfully retrieved table schema from {tool}."
        else:
            return f"Operation completed successfully using {tool}."


def parse_user_query(query: str, available_tools: dict, llm_model: str) -> dict: # Added llm_model
    """Parse user query with fully dynamic tool selection based on tool descriptions"""

    if not available_tools:
        return {"error": "No tools available"}

    # Build comprehensive tool information for the LLM
    tool_info = []
    for tool_name, tool_desc in available_tools.items():
        tool_info.append(f"- **{tool_name}**: {tool_desc}")

    tools_description = "\n".join(tool_info)

    system = (
        "You are an intelligent sales agent and database router for CRUD operations. "
        "Your job is to analyze the user's query and select the most appropriate tool based on the tool descriptions provided.\n\n"

        "AS A SALES AGENT, YOU SHOULD:\n"
        "- Understand business context and customer needs\n"
        "- Recognize sales-related queries (orders, transactions, revenue, customer purchases)\n"
        "- Identify cross-database relationships (customer orders, product sales, inventory)\n"
        "- Provide intelligent routing for business analytics and reporting needs\n"
        "- Handle complex queries that may involve multiple data sources\n\n"

        "RESPONSE FORMAT:\n"
        "Reply with exactly one JSON object: {\"tool\": string, \"action\": string, \"args\": object}\n\n"

        "ACTION MAPPING:\n"
        "- 'read': for viewing, listing, showing, displaying, or getting records\n"
        "- 'create': for adding, inserting, or creating new records (orders, customers, products)\n"
        "- 'update': for modifying, changing, or updating existing records\n"
        "- 'delete': for removing, deleting, or destroying records\n"
        "- 'describe': for showing table structure, schema, or column information\n\n"

        "TOOL SELECTION GUIDELINES:\n"
        "- Analyze the user's business intent and match it with the most relevant tool description\n"
        "- **Prioritize tools based on the primary data type being requested:**\n"
        "  * **Customers:** Use `sqlserver_crud` for managing customer records (e.g., 'add customer', 'list customers', 'update customer email').\n"
        "  * **Products:** Use `postgresql_crud` for managing product inventory and details (e.g., 'add product', 'list products', 'update product price').\n"
        "  * **Sales Transactions:** Use `sales_crud` for recording, reading, updating, or deleting sales and order history. This tool interacts with both customer and product data to complete a sale (e.g., 'record a sale', 'list sales', 'update sale quantity').\n"
        "- If a query involves both customer and product information but is primarily about a *transaction*, use `sales_crud`.\n"
        "- If multiple tools could work, choose the most specific one for the business context.\n\n"

        "SALES-SPECIFIC ROUTING:\n"
        "- 'show sales', 'list transactions', 'revenue report' → Use `sales_crud`\n"
        "- 'customer purchases', 'order history' → Use `sales_crud` with customer context\n"
        "- 'product sales', 'top selling items' → Use `sales_crud` with product context\n"
        "- 'create order', 'new sale' → Use `sales_crud`\n"
        "- 'customer list', 'add customer', 'update customer email' → Use `sqlserver_crud`\n"
        "- 'product catalog', 'inventory', 'add product', 'update product price' → Use `postgresql_crud`\n\n"

        "ARGUMENT EXTRACTION:\n"
        "- Extract relevant business parameters from the user query\n"
        "- For `sqlserver_crud` (customers): Use `first_name`, `last_name`, `email`, `customer_id`, `new_email`, `new_first_name`, `new_last_name`.\n"
        "- For `postgresql_crud` (products): Use `name`, `price`, `description`, `product_id`, `new_price`, `new_quantity`, `sales_amount`, `category`, `launch_date`, `new_category`, `new_launch_date`.\n"
        "- For `sales_crud` (sales): Use `customer_id`, `product_id`, `quantity`, `unit_price`, `total_amount`, `sale_id`, `new_quantity`.\n"
        "- For `describe`: include 'table_name' if mentioned (e.g., 'Customers', 'products', 'Sales').\n\n"
        "- **ETL Scenario Guidance for LLM (for create/update operations):**\n"
        "  - **Data Format Conversion (Dates):** If a date is provided (e.g., '31-07-2025', 'July 31st 2025'), extract it and format it as 'YYYY-MM-DD' for `launch_date` in `postgresql_crud`.\n"
        "  - **Decimal Value Formatting:** Ensure `price`, `unit_price`, `total_amount` are extracted as floats. The server will handle rounding to 2 decimal places.\n"
        "  - **String Concatenation (Names):** If a full customer name is provided (e.g., 'Jon Snow'), split it into `first_name` and `last_name` for `sqlserver_crud`. If only one name is given, use it as `first_name` and leave `last_name` empty.\n"
        "  - **Null Value Handling (Category):** If a new product is added and no `category` is specified, do NOT provide a `category` argument. Let the server's default 'Uncategorized' apply. If the user explicitly states 'no category' or 'uncategorized', pass 'Uncategorized'.\n\n"


        f"AVAILABLE TOOLS:\n{tools_description}\n\n"

        "BUSINESS EXAMPLES:\n"
        "Query: 'list all customers' → {\"tool\": \"sqlserver_crud\", \"action\": \"read\", \"args\": {}}\n"
        "Query: 'add customer John Doe with email john@example.com' → {\"tool\": \"sqlserver_crud\", \"action\": \"create\", \"args\": {\"first_name\": \"John\", \"last_name\": \"Doe\", \"email\": \"john@example.com\"}}\n"
        "Query: 'show product inventory' → {\"tool\": \"postgresql_crud\", \"action\": \"read\", \"args\": {}}\n"
        "Query: 'add product Laptop for $1200' → {\"tool\": \"postgresql_crud\", \"action\": \"create\", \"args\": {\"name\": \"Laptop\", \"price\": 1200.0}}\n"
        "Query: 'display sales report' → {\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {}}\n"
        "Query: 'record a sale for customer 1 and product 2, quantity 5, unit price 14.99' → {\"tool\": \"sales_crud\", \"action\": \"create\", \"args\": {\"customer_id\": 1, \"product_id\": 2, \"quantity\": 5, \"unit_price\": 14.99}}\n"
        "Query: 'update email for customer 1 to new@example.com' → {\"tool\": \"sqlserver_crud\", \"action\": \"update\", \"args\": {\"customer_id\": 1, \"new_email\": \"new@example.com\"}}\n"
        "Query: 'update product 1 price to 10.50' → {\"tool\": \"postgresql_crud\", \"action\": \"update\", \"args\": {\"product_id\": 1, \"new_price\": 10.50}}\n"
        "Query: 'update sale 1 quantity to 12' → {\"tool\": \"sales_crud\", \"action\": \"update\", \"args\": {\"sale_id\": 1, \"new_quantity\": 12}}\n"
        "Query: 'Add a product called SmartWatch with price 299.99 and category Wearables launched on 2024-05-20' -> {\"tool\": \"postgresql_crud\", \"action\": \"create\", \"args\": {\"name\": \"SmartWatch\", \"price\": 299.99, \"category\": \"Wearables\", \"launch_date\": \"2024-05-20\"}}\n"
        "Query: 'Add a product called EcoBike for 750.55' -> {\"tool\": \"postgresql_crud\", \"action\": \"create\", \"args\": {\"name\": \"EcoBike\", \"price\": 750.55}}\n"
        "Query: 'Update product 1 launch date to 15-08-2023' -> {\"tool\": \"postgresql_crud\", \"action\": \"update\", \"args\": {\"product_id\": 1, \"new_launch_date\": \"2023-08-15\"}}\n"
    )

    prompt = f"User query: \"{query}\"\n\nAs a sales agent, analyze the query and select the most appropriate tool based on the descriptions above. Consider the business context and data relationships. Respond with JSON only."

    try:
        if llm_model.startswith("Groq"):
            client = Groq(api_key=os.getenv("GROQ_API_KEY"))
            model_name = llm_model.split(" - ")[1] # Extract model name like "llama3-8b-8192"
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
            )
        else: # Fallback to OpenAI if not Groq (or other future LLMs)
            openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            model_name = llm_model.lower().replace(" ", "-") # Adjust model name for OpenAI

        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )

        raw = _clean_json(resp.choices[0].message.content)

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            result = ast.literal_eval(raw)

        # Normalize action names
        if "action" in result and result["action"] in ["list", "show", "display", "view", "get"]:
            result["action"] = "read"

        # Validate tool selection - if the LLM picks a non-existent tool, this will prevent errors
        if "tool" in result and result["tool"] not in available_tools:
            # Fallback or error handling for invalid tool selection
            return {"error": f"LLM selected an invalid tool: '{result['tool']}'. Please refine your query.", "tool_selection_error": True}


        return result

    except Exception as e:
        # Fallback response if LLM call fails
        st.error(f"LLM Parsing Error: {e}") # Display error for debugging
        return {
            "tool": list(available_tools.keys())[0] if available_tools else None, # Fallback to first available tool
            "action": "read",
            "args": {},
            "error": f"Failed to parse query: {str(e)}"
        }


async def _invoke_tool(tool: str, action: str, args: dict) -> any:
    # Ensure the URL is correctly formed for MCP server endpoint
    transport = StreamableHttpTransport(f"{st.session_state['MCP_SERVER_URL']}")
    async with Client(transport) as client:
        payload = {"operation": action, **{k: v for k, v in args.items() if k != "operation"}}
        # --- DEBUG PRINT ADDED HERE ---
        st.write(f"DEBUG: Payload being sent to MCP tool '{tool}': {payload}")
        # --- END DEBUG PRINT ---
        res_obj = await client.call_tool(tool, payload)
    if res_obj.structured_content is not None:
        return res_obj.structured_content
    text = "".join(b.text for b in res_obj.content).strip()
    if text.startswith("{") and "}{" in text:
        text = "[" + text.replace("}{", "},{") + "]"
    try:
        return json.loads(text)
    except:
        return text


def call_mcp_tool(tool: str, action: str, args: dict) -> any:
    return asyncio.run(_invoke_tool(tool, action, args))


def format_natural(data, display_option: str = "Default Formatting") -> str:
    """
    Formats the data for natural language display based on the selected display option.
    Applies ETL-like transformations for display purposes.
    """
    if isinstance(data, list):
        processed_items = []
        for item in data:
            if isinstance(item, dict):
                formatted_item = item.copy() # Work on a copy
                
                # --- Apply ETL Scenarios for Display ---
                if display_option == "Data Format Conversion":
                    # Convert dates to readable format
                    if "sale_date" in formatted_item and isinstance(formatted_item["sale_date"], datetime):
                        formatted_item["sale_date"] = formatted_item["sale_date"].strftime("%Y-%m-%d %H:%M:%S")
                    elif "sale_date" in formatted_item and isinstance(formatted_item["sale_date"], str):
                         try:
                             dt_obj = datetime.fromisoformat(formatted_item["sale_date"])
                             formatted_item["sale_date"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                         except ValueError:
                             pass # Keep as is if not a valid ISO format
                    
                    if "product_launch_date" in formatted_item and isinstance(formatted_item["product_launch_date"], str):
                        try:
                            # Assuming server sends ISO format 'YYYY-MM-DD' for date
                            date_obj = datetime.strptime(formatted_item["product_launch_date"], "%Y-%m-%d").date()
                            formatted_item["product_launch_date"] = date_obj.strftime("%d-%m-%Y") # Convert to DD-MM-YYYY
                        except ValueError:
                            pass # Keep original string if conversion fails
                    
                    if "CreatedAt" in formatted_item and isinstance(formatted_item["CreatedAt"], str):
                        try:
                            dt_obj = datetime.fromisoformat(formatted_item["CreatedAt"])
                            formatted_item["CreatedAt"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            pass # Keep as is if not a valid ISO format

                elif display_option == "Decimal Value Formatting":
                    for key in ["price", "unit_price", "total_price", "sales_amount"]:
                        if key in formatted_item and (isinstance(formatted_item[key], (float, Decimal, int))):
                            try:
                                formatted_item[key] = f"{Decimal(str(formatted_item[key])):.2f}"
                            except InvalidOperation:
                                pass # Keep as is if not a valid number
                
                elif display_option == "String Concatenation":
                    # FullName is now a generated column from DB, so it should already be there for customers
                    if "FirstName" in formatted_item and "LastName" in formatted_item and "FullName" not in formatted_item:
                        formatted_item["FullName"] = f"{formatted_item['FirstName']} {formatted_item['LastName']}".strip()
                        formatted_item.pop("FirstName", None)
                        formatted_item.pop("LastName", None)
                    
                    if "product_name" in formatted_item and "product_description_raw" in formatted_item and "product_category" in formatted_item:
                        formatted_item["product_full_description"] = (
                            f"{formatted_item['product_name']} "
                            f"({formatted_item['product_description_raw'] or 'No description'}) "
                            f"[Category: {formatted_item['product_category'] or 'N/A'}]"
                        )
                        formatted_item.pop("product_description_raw", None)
                        formatted_item.pop("product_category", None)
                    
                    if "customer_full_name" in formatted_item and "product_name" in formatted_item and "quantity" in formatted_item and "total_price" in formatted_item and "sale_date" in formatted_item:
                        sale_date_str = formatted_item["sale_date"].strftime("%Y-%m-%d") if isinstance(formatted_item["sale_date"], datetime) else str(formatted_item["sale_date"])
                        formatted_item["sale_summary"] = (
                            f"{formatted_item['customer_full_name']} bought {formatted_item['quantity']} of "
                            f"{formatted_item['product_name']} for ${formatted_item['total_price']:.2f} on {sale_date_str}"
                        )

                elif display_option == "Null Value Removal/Handling":
                    # Remove entries with None/null email for sales or customers
                    if "customer_email" in formatted_item and (formatted_item["customer_email"] is None or formatted_item["customer_email"] == "N/A"):
                        continue # Skip this record
                    
                    # Replace None/null descriptions or categories with 'N/A' or 'Uncategorized' for display
                    if "product_description_raw" in formatted_item and formatted_item["product_description_raw"] is None:
                        formatted_item["product_description_raw"] = "N/A"
                    if "product_category" in formatted_item and formatted_item["product_category"] is None:
                        formatted_item["product_category"] = "Uncategorized"
                    if "launch_date" in formatted_item and formatted_item["launch_date"] is None:
                        formatted_item["launch_date"] = "N/A"
                    if "description" in formatted_item and formatted_item["description"] is None:
                        formatted_item["description"] = "N/A"
                    if "Email" in formatted_item and formatted_item["Email"] is None:
                        formatted_item["Email"] = "N/A"


                # Construct string representation of the formatted item
                parts = []
                for k, v in formatted_item.items():
                    # Skip raw first/last name if full_name is present
                    if k in ["first_name", "last_name"] and "customer_full_name" in formatted_item:
                        continue
                    # Skip raw product description/category if product_full_description is present
                    if k in ["product_description_raw", "product_category"] and "product_full_description" in formatted_item:
                        continue
                    
                    if k == "sale_date" and isinstance(v, datetime):
                        parts.append(f"Sale Date: {v.strftime('%Y-%m-%d %H:%M:%S')}")
                    elif k == "product_launch_date" and isinstance(v, (datetime, date)):
                        parts.append(f"Launch Date: {v.strftime('%Y-%m-%d')}")
                    elif k == "product_launch_date" and isinstance(v, str): # For already converted strings
                        parts.append(f"Launch Date: {v}")
                    elif k == "CreatedAt" and isinstance(v, (datetime, date)):
                        parts.append(f"Created At: {v.strftime('%Y-%m-%d %H:%M:%S')}")
                    elif k == "CreatedAt" and isinstance(v, str): # For already converted strings
                        parts.append(f"Created At: {v}")
                    elif k == "unit_price" or k == "total_price" or k == "sales_amount" or k == "price":
                        if isinstance(v, (float, Decimal, int)):
                            parts.append(f"{k.replace('_', ' ').title()}: ${v:.2f}")
                        else: # Already formatted string
                            parts.append(f"{k.replace('_', ' ').title()}: ${v}")
                    elif k == "email":
                        parts.append(f"Email: {v or 'N/A'}")
                    elif k == "description" or k == "product_description_raw":
                        parts.append(f"Description: {v or 'N/A'}")
                    elif k == "category" or k == "product_category":
                        parts.append(f"Category: {v or 'Uncategorized'}")
                    elif k == "full_name" or k == "customer_full_name":
                        parts.append(f"Customer: {v}")
                    elif k == "product_full_description":
                        parts.append(f"Product: {v}")
                    elif k == "sale_summary":
                        parts.append(f"Summary: {v}")
                    else:
                        parts.append(f"{k.replace('_', ' ').title()}: {v}")
                processed_items.append(", ".join(parts))
            else:
                processed_items.append(str(item))
        return "\n".join(processed_items)
    elif isinstance(data, dict):
        # Apply formatting for single dictionary (e.g., describe results)
        formatted_item = data.copy()
        
        # Apply ETL Scenarios for Display (similar to list, but for single item)
        if display_option == "Data Format Conversion":
            if "sale_date" in formatted_item and isinstance(formatted_item["sale_date"], datetime):
                formatted_item["sale_date"] = formatted_item["sale_date"].strftime("%Y-%m-%d %H:%M:%S")
            elif "sale_date" in formatted_item and isinstance(formatted_item["sale_date"], str):
                try:
                    dt_obj = datetime.fromisoformat(formatted_item["sale_date"])
                    formatted_item["sale_date"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
            if "product_launch_date" in formatted_item and isinstance(formatted_item["product_launch_date"], str):
                try:
                    date_obj = datetime.strptime(formatted_item["product_launch_date"], "%Y-%m-%d").date()
                    formatted_item["product_launch_date"] = date_obj.strftime("%d-%m-%Y")
                except ValueError:
                    pass
            if "CreatedAt" in formatted_item and isinstance(formatted_item["CreatedAt"], str):
                try:
                    dt_obj = datetime.fromisoformat(formatted_item["CreatedAt"])
                    formatted_item["CreatedAt"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass

        elif display_option == "Decimal Value Formatting":
            for key in ["price", "unit_price", "total_price", "sales_amount"]:
                if key in formatted_item and (isinstance(formatted_item[key], (float, Decimal, int))):
                    try:
                        formatted_item[key] = f"{Decimal(str(formatted_item[key])):.2f}"
                    except InvalidOperation:
                        pass
        elif display_option == "String Concatenation":
            if "FirstName" in formatted_item and "LastName" in formatted_item and "FullName" not in formatted_item:
                formatted_item["FullName"] = f"{formatted_item['FirstName']} {formatted_item['LastName']}".strip()
                formatted_item.pop("FirstName", None)
                formatted_item.pop("LastName", None)
            if "product_name" in formatted_item and "product_description_raw" in formatted_item and "product_category" in formatted_item:
                formatted_item["product_full_description"] = (
                    f"{formatted_item['product_name']} "
                    f"({formatted_item['product_description_raw'] or 'No description'}) "
                    f"[Category: {formatted_item['product_category'] or 'N/A'}]"
                )
                formatted_item.pop("product_description_raw", None)
                formatted_item.pop("product_category", None)
            if "customer_full_name" in formatted_item and "product_name" in formatted_item and "quantity" in formatted_item and "total_price" in formatted_item and "sale_date" in formatted_item:
                sale_date_str = formatted_item["sale_date"].strftime("%Y-%m-%d") if isinstance(formatted_item["sale_date"], datetime) else str(formatted_item["sale_date"])
                formatted_item["sale_summary"] = (
                    f"{formatted_item['customer_full_name']} bought {formatted_item['quantity']} of "
                    f"{formatted_item['product_name']} for ${formatted_item['total_price']:.2f} on {sale_date_str}"
                )
        elif display_option == "Null Value Removal/Handling":
            if "customer_email" in formatted_item and (formatted_item["customer_email"] is None or formatted_item["customer_email"] == "N/A"):
                pass # This format is for filtering lists, not single items.
            if "product_description_raw" in formatted_item and formatted_item["product_description_raw"] is None:
                formatted_item["product_description_raw"] = "N/A"
            if "product_category" in formatted_item and formatted_item["product_category"] is None:
                formatted_item["product_category"] = "Uncategorized"
            if "launch_date" in formatted_item and formatted_item["launch_date"] is None:
                formatted_item["launch_date"] = "N/A"
            if "description" in formatted_item and formatted_item["description"] is None:
                formatted_item["description"] = "N/A"
            if "Email" in formatted_item and formatted_item["Email"] is None:
                formatted_item["Email"] = "N/A"


        parts = []
        for k, v in formatted_item.items():
            if k in ["first_name", "last_name"] and "customer_full_name" in formatted_item:
                continue
            if k in ["product_description_raw", "product_category"] and "product_full_description" in formatted_item:
                continue

            if k == "sale_date" and isinstance(v, datetime):
                parts.append(f"Sale Date: {v.strftime('%Y-%m-%d %H:%M:%S')}")
            elif k == "product_launch_date" and isinstance(v, (datetime, date)):
                parts.append(f"Launch Date: {v.strftime('%Y-%m-%d')}")
            elif k == "product_launch_date" and isinstance(v, str):
                parts.append(f"Launch Date: {v}")
            elif k == "CreatedAt" and isinstance(v, (datetime, date)):
                parts.append(f"Created At: {v.strftime('%Y-%m-%d %H:%M:%S')}")
            elif k == "CreatedAt" and isinstance(v, str):
                parts.append(f"Created At: {v}")
            elif k == "unit_price" or k == "total_price" or k == "sales_amount" or k == "price":
                if isinstance(v, (float, Decimal, int)):
                    parts.append(f"{k.replace('_', ' ').title()}: ${v:.2f}")
                else:
                    parts.append(f"{k.replace('_', ' ').title()}: ${v}")
            elif k == "email":
                parts.append(f"Email: {v or 'N/A'}")
            elif k == "description" or k == "product_description_raw":
                parts.append(f"Description: {v or 'N/A'}")
            elif k == "category" or k == "product_category":
                parts.append(f"Category: {v or 'Uncategorized'}")
            elif k == "full_name" or k == "customer_full_name":
                parts.append(f"Customer: {v}")
            elif k == "product_full_description":
                parts.append(f"Product: {v}")
            elif k == "sale_summary":
                parts.append(f"Summary: {v}")
            else:
                parts.append(f"{k.replace('_', ' ').title()}: {v}")
        return ", ".join(parts) + "."
    return str(data)


def normalize_args(args):
    # This function is now less critical as LLM prompt guides direct arg extraction
    # but kept for any potential edge cases or future needs.
    return args


# Define required arguments for create operations for conversational loop
REQUIRED_ARGS_FOR_CREATE = {
    "sqlserver_crud": ["first_name", "last_name"],
    "postgresql_crud": ["name", "price"],
    "sales_crud": ["customer_id", "product_id", "quantity", "unit_price"]
}

# Define how to extract missing info from a follow-up query
FIELD_EXTRACTION_MAP = {
    "first_name": lambda q: re.search(r'(?:first name is|first name)\s+([a-zA-Z]+)', q, re.IGNORECASE),
    "last_name": lambda q: re.search(r'(?:last name is|last name)\s+([a-zA-Z]+)', q, re.IGNORECASE),
    "email": lambda q: re.search(r'email\s+([\w\.-]+@[\w\.-]+)', q, re.IGNORECASE),
    "name": lambda q: re.search(r'(?:name is|name)\s+([a-zA-Z0-9\s]+)', q, re.IGNORECASE),
    "price": lambda q: re.search(r'(?:price is|price)\s+\$?(\d+(?:\.\d+)?)', q, re.IGNORECASE),
    "quantity": lambda q: re.search(r'(?:quantity is|quantity)\s+(\d+)', q, re.IGNORECASE),
    "unit_price": lambda q: re.search(r'(?:unit price is|unit price)\s+\$?(\d+(?:\.\d+)?)', q, re.IGNORECASE),
    "category": lambda q: re.search(r'(?:category is|category)\s+([a-zA-Z\s]+)', q, re.IGNORECASE),
    "launch_date": lambda q: re.search(r'(?:launch date is|launched on)\s+(\d{2}-\d{2}-\d{4}|\d{4}-\d{2}-\d{2})', q, re.IGNORECASE),
    "customer_id": lambda q: re.search(r'(?:customer id is|customer id)\s+(\d+)', q, re.IGNORECASE),
    "product_id": lambda q: re.search(r'(?:product id is|product id)\s+(\d+)', q, re.IGNORECASE),
}


def generate_table_description(df: pd.DataFrame, content: dict, action: str, tool: str, llm_model: str) -> str: # Added llm_model
    """Generate LLM-based table description from JSON response data"""

    # Sample first few rows for context (don't send all data to LLM)
    sample_data = df.head(3).to_dict('records') if len(df) > 0 else []

    # Create context for LLM
    context = {
        "action": action,
        "tool": tool,
        "record_count": len(df),
        "columns": list(df.columns) if len(df) > 0 else [],
        "sample_data": sample_data,
        "full_response": content.get("result", [])[:3] if isinstance(content.get("result"), list) else content.get(
            "result", "")
    }

    system_prompt = (
        "You are a data analyst. Generate a brief, insightful 1-line description "
        "of the table data based on the JSON response. Focus on what the data represents "
        "and any interesting patterns you notice. Be concise and business-focused."
    )

    prompt = f"""
    Analyze this table data and generate a single insightful line about it:

    Context: {json.dumps(context, indent=2)}

    Generate one line describing what this data represents and any key insights.
    """

    try:
        if llm_model.startswith("Groq"):
            client = Groq(api_key=os.getenv("GROQ_API_KEY"))
            model_name = llm_model.split(" - ")[1] # Extract model name
        else:
            # Ensure OpenAI API key is available if an OpenAI model is selected
            if not os.getenv("OPENAI_API_KEY"):
                raise ValueError("OPENAI_API_KEY environment variable not set.")
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            model_name = llm_model.lower().replace(" ", "-") # Adjust model name

        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=80
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        st.error(f"LLM Table Description Error: {e}") # Display error for debugging
        return f"Retrieved {len(df)} records from the database."


# ========== MAIN ==========
if application == "MCP Application":
    user_avatar_url = "[https://cdn-icons-png.flaticon.com/512/1946/1946429.png](https://cdn-icons-png.flaticon.com/512/1946/1946429.png)"
    agent_avatar_url = "[https://cdn-icons-png.flaticon.com/512/4712/4712039.png](https://cdn-icons-png.flaticon.com/512/4712/4712039.png)"

    MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000/mcp") 
    st.session_state["MCP_SERVER_URL"] = MCP_SERVER_URL

    # Generate dynamic tool descriptions
    TOOL_DESCRIPTIONS = generate_tool_descriptions(st.session_state.available_tools)

    # ========== TOOLS STATUS AND REFRESH BUTTON ==========
    # Create columns for tools info and refresh button
    col1, col2 = st.columns([4, 1])

    with col1:
        # Display discovered tools info
        if st.session_state.available_tools:
            st.info(
                f"🔧 Discovered {len(st.session_state.available_tools)} tools: {', '.join(st.session_state.available_tools.keys())}")
        else:
            st.warning("⚠️ No tools discovered. Please check your MCP server connection.")

    with col2:
        # Small refresh button on main page
        st.markdown('<div class="small-refresh-button">', unsafe_allow_html=True)
        if st.button("🔄 Refresh", key="refresh_tools_main", help="Rediscover available tools"):
            with st.spinner("Refreshing tools..."):
                MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000/mcp") 
                st.session_state["MCP_SERVER_URL"] = MCP_SERVER_URL
                discovered_tools = discover_tools()
                st.session_state.available_tools = discovered_tools
                st.session_state.tool_states = {tool: True for tool in discovered_tools.keys()}
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # ========== 1. RENDER CHAT MESSAGES ==========
    st.markdown('<div class="stChatPaddingBottom">', unsafe_allow_html=True)
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(
                f"""
                <div class="chat-row right">
                    <div class="chat-bubble user-msg user-bubble">{msg['content']}</div>
                    <img src="{user_avatar_url}" class="avatar user-avatar" alt="User">
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif msg.get("format") == "reasoning":
            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble"><i>{msg['content']}</i></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif msg.get("format") == "multi_step_read" and isinstance(msg["content"], dict):
            step = msg["content"]
            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble">
                        <b>Step: Lookup by name</b> (<code>{step['args'].get('name', '')}</code>)
                    </div>
                </div>
                """, unsafe_allow_html=True
            )
            with st.expander(f"Lookup Request: {step['tool']}"):
                st.code(json.dumps({
                    "tool": step['tool'],
                    "action": step['action'],
                    "args": step['args']
                }, indent=2), language="json")
            if isinstance(step["result"], dict) and "sql" in step["result"]:
                with st.expander("Lookup SQL Query Used"):
                    st.code(step["result"]["sql"], language="sql")
            if isinstance(step["result"], dict) and "result" in step["result"]:
                with st.expander("Lookup Response"):
                    st.code(json.dumps(step["result"]["result"], indent=2), language="json")
                    if isinstance(step["result"]["result"], list) and step["result"]["result"]:
                        df = pd.DataFrame(step["result"]["result"])
                        st.markdown("**Lookup Result Table:**")
                        st.table(df)
        elif msg.get("format") == "sql_crud" and isinstance(msg["content"], dict):
            content = msg["content"]
            action = msg.get("action", "")
            tool = msg.get("tool", "")
            user_query = msg.get("user_query", "")
            current_llm_model = st.session_state.get("llm_select", "Groq - Llama3-8b-8192") 
            selected_display_option = st.session_state.get("display_option_select", "Default Formatting")

            with st.expander("Details"):
                if "request" in msg:
                    st.markdown("**Request**")
                    st.code(json.dumps(msg["request"], indent=2), language="json")
                    st.markdown("---")
                st.markdown("**SQL Query Used**")
                st.code(content["sql"] or "No SQL executed", language="sql")
                st.markdown("---")
                st.markdown("**Response**")
                if isinstance(content["result"], (dict, list)):
                    st.code(json.dumps(content["result"], indent=2), language="json")
                else:
                    st.code(content["result"])

            # Generate LLM response for the operation, passing the selected LLM model
            llm_response = generate_llm_response(content, action, tool, user_query, current_llm_model)

            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble">{llm_response}</div>
                </div>
                """, unsafe_allow_html=True
            )

            if action in {"create", "update", "delete"}:
                result_msg = content.get("result", "")
                if "✅" in result_msg or "success" in result_msg.lower():
                    st.success(result_msg)
                elif "❌" in result_msg or "fail" in result_msg.lower() or "error" in result_msg.lower():
                    st.error(result_msg)
                else:
                    st.info(result_msg)
                try:
                    st.markdown("#### Here's the updated table after your operation:")
                    read_tool = tool
                    read_args = {}
                    updated_table = call_mcp_tool(read_tool, "read", read_args)
                    if isinstance(updated_table, dict) and "result" in updated_table:
                        # Apply formatting to the displayed table
                        # Filter out records with null email if Null Value Removal/Handling is selected
                        filtered_data = []
                        for row in updated_table["result"]:
                            if selected_display_option == "Null Value Removal/Handling" and "customer_email" in row and (row["customer_email"] is None or row["customer_email"] == "N/A"):
                                continue
                            if selected_display_option == "Null Value Removal/Handling" and "Email" in row and (row["Email"] is None or row["Email"] == "N/A"):
                                continue
                            filtered_data.append(row)

                        # Now apply formatting to the filtered data
                        formatted_table_data = []
                        for row in filtered_data:
                            try:
                                # ast.literal_eval expects a string representation of a Python literal (like a dict or list)
                                # format_natural returns a string like "Key: Value, Key2: Value2"
                                # We need to convert this back to a dict for DataFrame, so we need to parse it.
                                # A simple way is to make it look like a dict string and then eval it.
                                # This is a bit hacky, but given the current format_natural output, it's a workaround.
                                # A better long-term solution would be for format_natural to return a dict directly for table display.
                                formatted_str = format_natural(row, selected_display_option)
                                # Convert "Key: Value, Key2: Value2" into "{'Key': 'Value', 'Key2': 'Value2'}"
                                # This is a fragile conversion, assuming no commas or colons within values themselves.
                                temp_dict_str = "{" + ", ".join([f"'{p.split(': ', 1)[0]}': '{p.split(': ', 1)[1]}'" for p in formatted_str.split(', ')]) + "}"
                                formatted_table_data.append(ast.literal_eval(temp_dict_str))
                            except Exception as e:
                                st.warning(f"Failed to parse formatted row for table display: {e}. Original formatted string: {formatted_str}")
                                formatted_table_data.append(row) # Fallback to raw row

                        updated_df = pd.DataFrame(formatted_table_data)
                        st.table(updated_df)
                except Exception as fetch_err:
                    st.info(f"Could not retrieve updated table: {fetch_err}")

            if action == "read" and isinstance(content["result"], list):
                st.markdown("#### Here's the current table:")
                # Apply formatting to the displayed table
                formatted_table_data = []
                for row in content["result"]:
                    # Null Value Removal/Handling for read operations (filtering)
                    if selected_display_option == "Null Value Removal/Handling" and "customer_email" in row and (row["customer_email"] is None or row["customer_email"] == "N/A"):
                        continue
                    if selected_display_option == "Null Value Removal/Handling" and "Email" in row and (row["Email"] is None or row["Email"] == "N/A"):
                        continue
                    
                    try:
                        formatted_str = format_natural(row, selected_display_option)
                        temp_dict_str = "{" + ", ".join([f"'{p.split(': ', 1)[0]}': '{p.split(': ', 1)[1]}'" for p in formatted_str.split(', ')]) + "}"
                        formatted_table_data.append(ast.literal_eval(temp_dict_str))
                    except Exception as e:
                        st.warning(f"Failed to parse formatted row for table display: {e}. Original formatted string: {formatted_str}")
                        formatted_table_data.append(row) # Fallback to raw row
                
                df = pd.DataFrame(formatted_table_data)
                st.table(df)
                st.markdown(f"The table contains {len(df)} records with '{selected_display_option}' applied.")
            elif action == "describe" and isinstance(content['result'], list):
                st.markdown("#### Table Schema: ")
                df = pd.DataFrame(content['result'])
                st.table(df)
                st.markdown(
                    "This shows the column names, data types, nullability, and maximum length for each column in the table.")
        else:
            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble">{msg['content']}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown('</div>', unsafe_allow_html=True)  # End stChatPaddingBottom

    # ========== 2. CLAUDE-STYLE STICKY CHAT BAR ==========
    st.markdown('<div class="sticky-chatbar"><div class="chatbar-claude">', unsafe_allow_html=True)
    with st.form("chatbar_form", clear_on_submit=True):
        chatbar_cols = st.columns([1, 16, 1])  # Left: hamburger, Middle: input, Right: send

        # --- LEFT: Hamburger (Tools) ---
        with chatbar_cols[0]:
            hamburger_clicked = st.form_submit_button("≡", use_container_width=True)

        # --- MIDDLE: Input Box ---
        with chatbar_cols[1]:
            user_query_input = st.text_input(
                "",
                placeholder="How can I help you today?",
                label_visibility="collapsed",
                key="chat_input_box"
            )

        # --- RIGHT: Send Button ---
        with chatbar_cols[2]:
            send_clicked = st.form_submit_button("➤", use_container_width=True)
    st.markdown('</div></div>', unsafe_allow_html=True)

    # ========== FLOATING TOOL MENU ==========
    if st.session_state.get("show_menu", False):
        st.markdown('<div class="tool-menu">', unsafe_allow_html=True)
        st.markdown('<div class="server-title">MultiDBCRUD</div>', unsafe_allow_html=True)
        tool_label = "Tools" + (" ▼" if st.session_state["menu_expanded"] else " ▶")
        if st.button(tool_label, key="expand_tools", help="Show tools", use_container_width=True):
            st.session_state["menu_expanded"] = not st.session_state["menu_expanded"]
        if st.session_state["menu_expanded"]:
            st.markdown('<div class="expandable">', unsafe_allow_html=True)
            for tool in st.session_state.tool_states.keys():
                enabled = st.session_state.tool_states[tool]
                new_val = st.toggle(tool, value=enabled, key=f"tool_toggle_{tool}")
                st.session_state.tool_states[tool] = new_val
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ========== PROCESS CHAT INPUT ==========
    if send_clicked and user_query_input:
        user_query = user_query_input
        user_steps = []
        current_llm_model = st.session_state.get("llm_select", "Groq - Llama3-8b-8192") 
        selected_display_option = st.session_state.get("display_option_select", "Default Formatting")

        # Handle conversational loop for missing fields
        if st.session_state.pending_tool_call and st.session_state.awaiting_input_for_field:
            field_name = st.session_state.awaiting_input_for_field
            extracted_value = None

            # Attempt to extract the value for the pending field from the new user query
            if field_name == "first_name" or field_name == "last_name":
                # Special handling for full name input
                name_parts = user_query.strip().split(maxsplit=1)
                if len(name_parts) == 2:
                    st.session_state.pending_tool_call["args"]["first_name"] = name_parts[0]
                    st.session_state.pending_tool_call["args"]["last_name"] = name_parts[1]
                    extracted_value = True # Indicate success
                elif len(name_parts) == 1:
                    st.session_state.pending_tool_call["args"]["first_name"] = name_parts[0]
                    st.session_state.pending_tool_call["args"]["last_name"] = "" # Empty last name
                    extracted_value = True
            else:
                extractor = FIELD_EXTRACTION_MAP.get(field_name)
                if extractor:
                    match = extractor(user_query)
                    if match:
                        extracted_value = match.group(1)
                        if field_name in ["price", "quantity", "unit_price"]:
                            try:
                                extracted_value = float(extracted_value)
                            except ValueError:
                                extracted_value = None # Failed to convert to number
                        # Handle date format for launch_date if it's the pending field
                        if field_name == "launch_date" and extracted_value:
                            try:
                                # Convert DD-MM-YYYY to YYYY-MM-DD for server
                                if re.match(r'\d{2}-\d{2}-\d{4}', extracted_value):
                                    day, month, year = extracted_value.split('-')
                                    extracted_value = f"{year}-{month}-{day}"
                            except Exception:
                                pass # Keep original if conversion fails
                        st.session_state.pending_tool_call["args"][field_name] = extracted_value

            if extracted_value is not None:
                # Clear pending state and proceed with the stored tool call
                p = st.session_state.pending_tool_call
                st.session_state.pending_tool_call = None
                st.session_state.awaiting_input_for_field = None
                st.session_state.messages.append({
                    "role": "user",
                    "content": user_query,
                    "format": "text",
                })
                # Now try to execute the tool call
                try_execute_tool_call(p, user_query, current_llm_model, selected_display_option)
                st.rerun()
            else:
                st.session_state.messages.append({
                    "role": "user",
                    "content": user_query,
                    "format": "text",
                })
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"I still need the '{field_name.replace('_', ' ')}'. Please provide it clearly.",
                    "format": "text",
                })
                st.rerun()
            return # Exit to avoid re-parsing the query

        # If not in a pending state, parse the new query
        try:
            enabled_tools = [k for k, v in st.session_state.tool_states.items() if v]
            if not enabled_tools:
                raise Exception("No tools are enabled. Please enable at least one tool in the menu.")

            p = parse_user_query(user_query, st.session_state.available_tools, current_llm_model)
            
            if "error" in p and p.get("tool_selection_error"):
                raise Exception(p["error"])

            tool = p.get("tool")
            if tool not in enabled_tools:
                raise Exception(f"Tool '{tool}' is disabled. Please enable it in the menu.")
            if tool not in st.session_state.available_tools:
                raise Exception(
                    f"Tool '{tool}' is not available. Available tools: {', '.join(st.session_state.available_tools.keys())}")

            action = p.get("action")
            args = p.get("args", {})
            p["args"] = args

            # Special handling for customer_name to split into first_name and last_name
            if tool == "sqlserver_crud" and action == "create":
                if "name" in args: # If user provides full name for create
                    customer_full_name = args.pop("name")
                    name_parts = customer_full_name.split(maxsplit=1)
                    args["first_name"] = name_parts[0]
                    args["last_name"] = name_parts[1] if len(name_parts) > 1 else ""
                    p["args"] = args

            # Validate required fields for 'create' operations
            if action == "create" and tool in REQUIRED_ARGS_FOR_CREATE:
                missing_fields = [field for field in REQUIRED_ARGS_FOR_CREATE[tool] if field not in args or args[field] is None]
                if missing_fields:
                    # Store the incomplete tool call
                    st.session_state.pending_tool_call = p
                    st.session_state.awaiting_input_for_field = missing_fields[0] # Ask for the first missing field
                    
                    st.session_state.messages.append({
                        "role": "user",
                        "content": user_query,
                        "format": "text",
                    })
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"I need more information to {action} a {tool.replace('_crud', '').replace('sqlserver', 'customer').replace('postgresql', 'product')}. Please provide the '{missing_fields[0].replace('_', ' ')}'.",
                        "format": "text",
                    })
                    st.rerun()
                    return # Exit to wait for next input

            # SQL Server: update by name
            if tool == "sqlserver_crud" and action == "update":
                if "name" in args: 
                    customer_full_name = args.pop("name")
                    name_parts = customer_full_name.split(maxsplit=1)
                    first_name = name_parts[0]
                    last_name = name_parts[1] if len(name_parts) > 1 else ""

                    read_args = {"first_name": first_name, "last_name": last_name}
                    read_result = call_mcp_tool(tool, "read", read_args)
                    if isinstance(read_result, dict) and "result" in read_result and read_result["result"]:
                        matches = [r for r in read_result["result"] if
                                   r.get("FirstName", "").lower() == first_name.lower() and
                                   r.get("LastName", "").lower() == last_name.lower()]
                        if matches:
                            args["customer_id"] = matches[0]["Id"]
                            p["args"] = args
                
                if "customer_id" not in args and "name" in args: 
                    read_args = {"name": args["name"]} 
                    read_result = call_mcp_tool(tool, "read", read_args)
                    if isinstance(read_result, dict) and "result" in read_result:
                        matches = [r for r in read_result["result"] if
                                   (r.get("FirstName", "") + " " + r.get("LastName", "")).lower() == args["name"].lower()]
                        if matches:
                            args["customer_id"] = matches[0]["Id"]
                            p["args"] = args

                if "new_email" not in args:
                    possible_email = re.search(r'to\s+([\w\.-]+@[\w\.-]+)', user_query, re.IGNORECASE)
                    if possible_email:
                        args["new_email"] = possible_email.group(1)
                        p["args"] = args

            # PostgreSQL: update by name
            if tool == "postgresql_crud" and action == "update":
                if "product_id" not in args and "name" in args:
                    read_args = {"name": args["name"]}
                    read_result = call_mcp_tool(tool, "read", read_args)
                    if isinstance(read_result, dict) and "result" in read_result:
                        matches = [r for r in read_result["result"] if
                                   r.get("name", "").lower() == args["name"].lower()]
                        if matches:
                            args["product_id"] = matches[0]["id"]
                            p["args"] = args
                if "name" not in args:
                    m = re.search(r'price of ([a-zA-Z0-9_ ]+?) (?:to|=)', user_query, re.I)
                    if m:
                        args["name"] = m.group(1).strip()
                        p["args"] = args
                if "new_price" not in args:
                    possible_price = re.search(r'(?:to|=)\s*\$?(\d+(?:\.\d+)?)', user_query)
                    if possible_price:
                        args['new_price'] = float(possible_price.group(1))
                        p["args"] = args
                if "new_launch_date" not in args:
                    possible_date = re.search(r'(?:date to|on)\s+(\d{2}-\d{2}-\d{4}|\d{4}-\d{2}-\d{2})', user_query, re.IGNORECASE)
                    if possible_date:
                        date_str = possible_date.group(1)
                        # Convert DD-MM-YYYY to YYYY-MM-DD for server
                        if re.match(r'\d{2}-\d{2}-\d{4}', date_str):
                            day, month, year = date_str.split('-')
                            date_str = f"{year}-{month}-{day}"
                        args['new_launch_date'] = date_str
                        p["args"] = args

            if tool == "postgresql_crud" and action == "delete":
                if "product_id" not in args and "name" in args:
                    read_args = {"name": args["name"]}
                    read_result = call_mcp_tool(tool, "read", read_args)
                    if isinstance(read_result, dict) and "result" in read_result:
                        matches = [r for r in read_result["result"] if
                                   r.get("name", "").lower() == args["name"].lower()]
                        if matches:
                            args["product_id"] = matches[0]["id"]
                            p["args"] = args
                if "product_id" not in args:
                    match = re.search(r'product\s+(\w+)', user_query, re.IGNORECASE)
                    if match:
                        product_name = match.group(1)
                        read_args = {"name": product_name}
                        read_result = call_mcp_tool(tool, "read", read_args)
                        if isinstance(read_result, dict) and "result" in read_result:
                            matches = [r for r in read_result["result"] if
                                       r.get("name", "").lower() == product_name.lower()]
                            if matches:
                                args["product_id"] = matches[0]["id"]
                                p["args"] = args
            
            # If all checks pass, execute the tool call
            try_execute_tool_call(p, user_query, current_llm_model, selected_display_option)

        except Exception as e:
            reply, fmt = f"⚠️ Error: {e}", "text"
            assistant_message = {
                "role": "assistant",
                "content": reply,
                "format": fmt,
            }
            st.session_state.messages.append({
                "role": "user",
                "content": user_query,
                "format": "text",
            })
            st.session_state.messages.append(assistant_message)
        st.rerun()  # Rerun so chat output appears

    # ========== Function to execute tool call and update messages ==========
    def try_execute_tool_call(p: dict, user_query: str, current_llm_model: str, selected_display_option: str):
        try:
            # Do NOT pass display_format to the server
            raw = call_mcp_tool(p["tool"], p["action"], p.get("args", {}))
        except Exception as e:
            reply, fmt = f"⚠️ Error calling tool '{p.get('tool')}': {e}", "text"
            assistant_message = {
                "role": "assistant",
                "content": reply,
                "format": fmt,
            }
            st.session_state.messages.append(assistant_message)
            return

        if isinstance(raw, dict) and "sql" in raw and "result" in raw:
            # For SQL CRUD results, apply formatting to the 'result' part
            if isinstance(raw["result"], list):
                # Filter out records with null email if Null Value Removal/Handling is selected
                filtered_data = []
                for row in raw["result"]:
                    if selected_display_option == "Null Value Removal/Handling" and "customer_email" in row and (row["customer_email"] is None or row["customer_email"] == "N/A"):
                        continue
                    if selected_display_option == "Null Value Removal/Handling" and "Email" in row and (row["Email"] is None or row["Email"] == "N/A"):
                        continue
                    filtered_data.append(row)

                reply_content = {
                    "sql": raw["sql"],
                    "result": [] # Initialize as empty list
                }
                for item in filtered_data:
                    try:
                        # format_natural returns a string like "Key: Value, Key2: Value2."
                        # We need to convert this back to a dictionary for display in st.table.
                        formatted_str = format_natural(item, selected_display_option)
                        
                        # This regex attempts to parse "Key: Value" pairs from the string.
                        # It's robust to spaces and handles the trailing period.
                        parsed_dict = {}
                        # Remove trailing period if present
                        clean_str = formatted_str.strip()
                        if clean_str.endswith('.'):
                            clean_str = clean_str[:-1]

                        pairs = re.findall(r"([^:]+):\s*([^,]+)(?:,\s*|$)", clean_str)
                        for key_raw, value_raw in pairs:
                            key = key_raw.strip().replace(" ", "_").lower() # Normalize key names
                            value = value_raw.strip()
                            # Attempt to convert to appropriate types if possible
                            if value.replace('.', '', 1).isdigit(): # Check if it's a number (int or float)
                                if '.' in value:
                                    parsed_dict[key] = float(value)
                                else:
                                    parsed_dict[key] = int(value)
                            elif value.lower() in ['true', 'false']:
                                parsed_dict[key] = value.lower() == 'true'
                            else:
                                parsed_dict[key] = value
                        
                        reply_content["result"].append(parsed_dict)
                    except Exception as e:
                        st.warning(f"Failed to parse formatted row for table display: {e}. Original formatted string: {formatted_str}")
                        reply_content["result"].append(item) # Fallback to raw item if parsing fails

            elif isinstance(raw["result"], dict):
                # Apply formatting for single dictionary results (e.g., describe)
                reply_content = {
                    "sql": raw["sql"],
                    "result": {} # Initialize as empty dict
                }
                try:
                    formatted_str = format_natural(raw["result"], selected_display_option)
                    clean_str = formatted_str.strip()
                    if clean_str.endswith('.'):
                        clean_str = clean_str[:-1]
                    
                    parsed_dict = {}
                    pairs = re.findall(r"([^:]+):\s*([^,]+)(?:,\s*|$)", clean_str)
                    for key_raw, value_raw in pairs:
                        key = key_raw.strip().replace(" ", "_").lower()
                        value = value_raw.strip()
                        if value.replace('.', '', 1).isdigit():
                            if '.' in value:
                                parsed_dict[key] = float(value)
                            else:
                                parsed_dict[key] = int(value)
                        elif value.lower() in ['true', 'false']:
                            parsed_dict[key] = value.lower() == 'true'
                        else:
                            parsed_dict[key] = value
                    reply_content["result"] = parsed_dict
                except Exception as e:
                    st.warning(f"Failed to parse formatted dict for table display: {e}. Original formatted string: {formatted_str}")
                    reply_content["result"] = raw["result"] # Fallback to raw dict
            else:
                reply_content = raw # Fallback for non-list/dict results
            fmt = "sql_crud"
        else:
            reply_content, fmt = format_natural(raw, selected_display_option), "text" # Apply formatting here

        assistant_message = {
            "role": "assistant",
            "content": reply_content,
            "format": fmt,
            "request": p,
            "tool": p.get("tool"),
            "action": p.get("action"),
            "args": p.get("args"),
            "user_query": user_query,
        }
        st.session_state.messages.append(assistant_message)

    # ========== 4. AUTO-SCROLL TO BOTTOM ==========
    components.html("""
        <script>
          setTimeout(() => { window.scrollTo(0, document.body.scrollHeight); }, 80);
        </script>
    """)


