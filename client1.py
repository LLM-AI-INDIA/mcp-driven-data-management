import os, re, json, ast, asyncio
import pandas as pd
import streamlit as st
import base64
from io import BytesIO
from PIL import Image
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
import streamlit.components.v1 as components
import re
from dotenv import load_dotenv

load_dotenv()

# Initialize Groq client with environment variable
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("🔐 GROQ_API_KEY environment variable is not set. Please add it to your environment.")
    st.stop()

groq_client = ChatGroq(
    groq_api_key=GROQ_API_KEY,
    model_name=os.environ.get("GROQ_MODEL", "llama3-70b-8192")
)

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
        transport = StreamableHttpTransport(f"{st.session_state.get('MCP_SERVER_URL', 'http://localhost:8000')}/mcp")
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
        llm_options = ["", "Groq Llama3-70B", "Groq Llama3-8B", "Groq Mixtral-8x7B", "Groq Gemma"]

        # Logic to auto-select defaults if MCP Application is chosen
        protocol_index = protocol_options.index(
            "MCP Protocol") if application == "MCP Application" else protocol_options.index(
            st.session_state.get("protocol_select", ""))
        llm_index = llm_options.index("Groq Llama3-70B") if application == "MCP Application" else llm_options.index(
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
            server_tools_options = ["", "sqlserver_crud", "postgresql_crud"]  # Fallback
            server_tools_index = 0

        server_tools = st.selectbox(
            "Server Tools",
            server_tools_options,
            key="server_tools",
            index=server_tools_index
        )

        st.button("Clear/Reset", key="clear_button")

    st.markdown('<div class="sidebar-logo-label">Build & Deployed on</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="sidebar-logo-row">
            <img src="https://media.licdn.com/dms/image/v2/D560BAQFIon13R1UG4g/company-logo_200_200/company-logo_200_200/0/1733990910443/llm_at_scale_logo?e=2147483647&v=beta&t=WtAgFOcGQuTS0aEIqZhNMzWraHwL6FU0z5EPyPrty04" title="LLM at Scale AI">
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
logo_path = "Picture1.png"
col1, col2, col3 = st.columns([1, 4, 1])
with col2:
    st.image(logo_path, width=180)
    
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
           MCP-Driven Data Management With Natural Language
        </span>
        <span style="
            font-size: 1.15rem;
            color: #555;
            margin-top: 0.35rem;
        ">
            Agentic Approach:  NO SQL, NO ETL, NO DATA WAREHOUSING, NO BI TOOL 
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
    st.session_state["MCP_SERVER_URL"] = os.getenv("MCP_SERVER_URL", "http://localhost:8000")

# Initialize tool_states dynamically based on discovered tools
if "tool_states" not in st.session_state:
    st.session_state.tool_states = {}

if "show_menu" not in st.session_state:
    st.session_state["show_menu"] = False
if "menu_expanded" not in st.session_state:
    st.session_state["menu_expanded"] = True
if "chat_input_box" not in st.session_state:
    st.session_state["chat_input_box"] = ""


# ========== HELPER FUNCTIONS ==========
def _clean_json(raw: str) -> str:
    fences = re.findall(r"``````", raw, re.DOTALL)
    if fences:
        return fences[0].strip()
    # If no JSON code fence, try to find JSON-like content
    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    return json_match.group(0).strip() if json_match else raw.strip()


# ========== PARAMETER VALIDATION FUNCTION ==========
def validate_and_clean_parameters(tool_name: str, args: dict) -> dict:
    """Validate and clean parameters for specific tools"""

    if tool_name == "sales_crud":
        # Define allowed parameters for sales_crud
        allowed_params = {
            'operation', 'customer_id', 'product_id', 'quantity',
            'unit_price', 'total_amount', 'sale_id', 'new_quantity',
            'table_name', 'display_format', 'customer_name',
            'product_name', 'email', 'total_price', 'columns',
            'where_condition', 'new_price'  # NEW PARAMETERS ADDED
        }

        # Clean args to only include allowed parameters
        cleaned_args = {k: v for k, v in args.items() if k in allowed_params}

        # Validate display_format values
        if 'display_format' in cleaned_args:
            valid_formats = [
                'Data Format Conversion',
                'Decimal Value Formatting',
                'String Concatenation',
                'Null Value Removal/Handling'
            ]
            if cleaned_args['display_format'] not in valid_formats:
                # Remove invalid display_format
                cleaned_args.pop('display_format', None)

        return cleaned_args

    elif tool_name == "sqlserver_crud":
        allowed_params = {
            'operation', 'name', 'email', 'limit', 'customer_id',
            'new_email', 'table_name'
        }
        return {k: v for k, v in args.items() if k in allowed_params}

    elif tool_name == "postgresql_crud":
        allowed_params = {
            'operation', 'name', 'price', 'description', 'limit',
            'product_id', 'new_price', 'table_name'
        }
        return {k: v for k, v in args.items() if k in allowed_params}

    return args


# ========== NEW LLM RESPONSE GENERATOR ==========
def generate_llm_response(operation_result: dict, action: str, tool: str, user_query: str) -> str:
    """Generate LLM response based on operation result with context"""

    # Prepare context for LLM
    context = {
        "action": action,
        "tool": tool,
        "user_query": user_query,
        "operation_result": operation_result
    }

    system_prompt = (
        "You are a helpful database assistant. Generate a brief, natural response "
        "explaining what operation was performed and its result. Be conversational "
        "and informative. Focus on the business context and user-friendly explanation."
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
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        response = groq_client.invoke(messages)
        return response.content.strip()
    except Exception as e:
        # Fallback response if LLM call fails
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


def parse_user_query(query: str, available_tools: dict) -> dict:
    """Parse user query with fully dynamic tool selection and new WHERE/column filtering support"""

    if not available_tools:
        return {"error": "No tools available"}

    # Build comprehensive tool information for the LLM
    tool_info = []
    for tool_name, tool_desc in available_tools.items():
        tool_info.append(f"- **{tool_name}**: {tool_desc}")

    tools_description = "\n".join(tool_info)

    system_prompt = (
        "You are an intelligent sales agent and database router for CRUD operations. "
        "Your job is to analyze the user's query and select the most appropriate tool based on the tool descriptions provided.\n\n"

        "RESPONSE FORMAT:\n"
        "Reply with exactly one JSON object: {\"tool\": string, \"action\": string, \"args\": object}\n\n"

        "ACTION MAPPING:\n"
        "- 'read': for viewing, listing, showing, displaying, or getting records\n"
        "- 'create': for adding, inserting, or creating new records (orders, customers, products)\n"
        "- 'update': for modifying, changing, or updating existing records\n"
        "- 'delete': for removing, deleting, or destroying records\n"
        "- 'describe': for showing table structure, schema, or column information\n\n"

        "NEW FEATURES - COLUMN FILTERING & WHERE CLAUSES:\n"
        "For sales_crud tool, support these advanced parameters:\n\n"

        "COLUMN FILTERING:\n"
        "- 'columns': comma-separated list of specific columns to return\n"
        "- Look for patterns like: 'show only product_name and quantity', 'select customer name and price'\n"
        "- Available columns: 'sale_id', 'customer_first_name', 'customer_last_name', 'product_name', 'product_description', 'quantity', 'unit_price', 'total_price', 'sale_date', 'customer_email'\n"
        "- Examples:\n"
        "  - 'show product names and quantities' → columns: 'product_name,quantity'\n"
        "  - 'display customer names and prices' → columns: 'customer_first_name,customer_last_name,total_price'\n\n"

        "WHERE CLAUSE SUPPORT:\n"
        "- 'where_condition': SQL WHERE clause conditions for filtering\n"
        "- For READ operations: filter displayed results\n"
        "- For UPDATE/DELETE operations: bulk operations on matching records\n"
        "- Examples:\n"
        "  - 'sales where price > 14' → where_condition: 's.unit_price > 14'\n"
        "  - 'update sales where quantity >= 2' → where_condition: 'quantity >= 2'\n"
        "  - 'delete sales where customer_id = 1' → where_condition: 'customer_id = 1'\n"
        "  - 'show sales for Alice' → where_condition: 'c.FirstName = \"Alice\"'\n\n"

        "WHERE CONDITION PATTERNS:\n"
        "- Price conditions: 'price > X', 'unit_price = X', 'total_price BETWEEN X AND Y'\n"
        "- Quantity conditions: 'quantity >= X', 'quantity = X'\n"
        "- Customer conditions: 'customer_id = X', 'c.FirstName = \"NAME\"'\n"
        "- Product conditions: 'product_id = X', 'p.name = \"PRODUCT\"'\n"
        "- Date conditions: 'sale_date >= \"YYYY-MM-DD\"'\n\n"

        "BULK OPERATIONS WITH WHERE:\n"
        "- 'update sales where price > 14 set quantity to 5' → action: 'update', where_condition: 'unit_price > 14', new_quantity: 5\n"
        "- 'delete sales where quantity = 1' → action: 'delete', where_condition: 'quantity = 1'\n"
        "- 'update price to 20 where product is Widget' → action: 'update', where_condition: 'p.name = \"Widget\"', new_price: 20\n\n"

        "ENHANCED NAME DETECTION FOR DELETE/UPDATE OPERATIONS:\n"
        "When user mentions delete/update operations with names, always extract and include 'name' parameter:\n"
        "Examples:\n"
        "- 'delete customer Alice' → extract 'name': 'Alice'\n"
        "- 'delete Alice Johnson' → extract 'name': 'Alice Johnson'\n"
        "- 'delete Johnson' → extract 'name': 'Johnson'\n"
        "- 'remove customer Bob' → extract 'name': 'Bob'\n"
        "- 'delete product Widget' → extract 'name': 'Widget'\n"
        "- 'update price of Gadget' → extract 'name': 'Gadget'\n"
        "- 'change price of Tool to 30' → extract 'name': 'Tool' and 'new_price': 30\n\n"

        "SALES ETL DISPLAY FORMATTING:\n"
        "For sales_crud tool, when user requests data formatting or conversion, use these EXACT display_format values:\n"
        "- 'Data Format Conversion' - for date formatting and data conversion\n"
        "- 'Decimal Value Formatting' - for price formatting to 2 decimal places\n"
        "- 'String Concatenation' - for combining fields into readable summaries\n"
        "- 'Null Value Removal/Handling' - for cleaning null values\n\n"

        "TOOL SELECTION GUIDELINES:\n"
        "- Analyze the user's business intent and match it with the most relevant tool description\n"
        "- Consider what type of data the user is asking about:\n"
        "  * Customer data: names, emails, contact information, customer management\n"
        "  * Product data: inventory, catalog, pricing, product details\n"
        "  * Sales data: transactions, orders, revenue, purchase history, analytics\n"
        "- Choose the tool whose description best matches the user's request\n"
        "- For sales queries, prioritize tools that handle transaction and sales data\n\n"

        "ARGUMENT EXTRACTION:\n"
        "- For sales operations: extract customer_id, product_id, quantity, unit_price\n"
        "- For updates: include new_quantity, sale_id, new_email, new_price\n"
        "- For ETL/formatting: include display_format with exact string match\n"
        "- For column filtering: include 'columns' parameter\n"
        "- For WHERE clauses: include 'where_condition' parameter\n"
        "- For delete/update by name: ALWAYS include 'name' parameter when names are mentioned\n\n"

        f"AVAILABLE TOOLS:\n{tools_description}\n\n"

        "EXAMPLES WITH NEW FEATURES:\n"
        "Query: 'show sales with only product name and quantity' → {{\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {{\"columns\": \"product_name,quantity\"}}}}\n"
        "Query: 'display sales where price > 15' → {{\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {{\"where_condition\": \"s.unit_price > 15\"}}}}\n"
        "Query: 'show customer names and prices for expensive sales' → {{\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {{\"columns\": \"customer_first_name,customer_last_name,total_price\", \"where_condition\": \"s.total_price > 20\"}}}}\n"
        "Query: 'update sales where price > 14 set quantity to 3' → {{\"tool\": \"sales_crud\", \"action\": \"update\", \"args\": {{\"where_condition\": \"unit_price > 14\", \"new_quantity\": 3}}}}\n"
        "Query: 'delete sales where quantity = 1' → {{\"tool\": \"sales_crud\", \"action\": \"delete\", \"args\": {{\"where_condition\": \"quantity = 1\"}}}}\n"
        "Query: 'delete customer Alice' → {{\"tool\": \"sqlserver_crud\", \"action\": \"delete\", \"args\": {{\"name\": \"Alice\"}}}}\n"
        "Query: 'show all sales' → {{\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {{}}}}\n"
    )

    user_prompt = f"User query: \"{query}\"\n\nAnalyze the query and respond with the exact JSON format. Extract column filtering, WHERE conditions, and names for operations."

    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        resp = groq_client.invoke(messages)

        raw = _clean_json(resp.content)

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            try:
                result = ast.literal_eval(raw)
            except:
                # Last resort parsing
                result = {"tool": list(available_tools.keys())[0], "action": "read", "args": {}}

        # Normalize action names
        if "action" in result and result["action"] in ["list", "show", "display", "view", "get"]:
            result["action"] = "read"

        # Validate and clean args - remove any unexpected parameters
        if "args" in result and isinstance(result["args"], dict):
            cleaned_args = validate_and_clean_parameters(result.get("tool"), result["args"])
            result["args"] = cleaned_args

        # Validate tool selection
        if "tool" in result and result["tool"] not in available_tools:
            result["tool"] = list(available_tools.keys())[0]

        return result

    except Exception as e:
        # Fallback response if LLM call fails
        return {
            "tool": list(available_tools.keys())[0] if available_tools else None,
            "action": "read",
            "args": {},
            "error": f"Failed to parse query: {str(e)}"
        }


async def _invoke_tool(tool: str, action: str, args: dict) -> any:
    transport = StreamableHttpTransport(f"{st.session_state['MCP_SERVER_URL']}/mcp")
    async with Client(transport) as client:
        payload = {"operation": action, **{k: v for k, v in args.items() if k != "operation"}}
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


def format_natural(data) -> str:
    if isinstance(data, list):
        lines = []
        for i, item in enumerate(data, 1):
            if isinstance(item, dict):
                parts = [f"{k} {v}" for k, v in item.items()]
                lines.append(f"Record {i}: " + ", ".join(parts) + ".")
            else:
                lines.append(f"{i}. {item}")
        return "\n".join(lines)
    if isinstance(data, dict):
        parts = [f"{k} {v}" for k, v in data.items()]
        return ", ".join(parts) + "."
    return str(data)


def normalize_args(args):
    mapping = {
        "product_name": "name",
        "customer_name": "name",
        "item": "name"
    }
    for old_key, new_key in mapping.items():
        if old_key in args:
            args[new_key] = args.pop(old_key)
    return args


def extract_name_from_query(text: str) -> str:
    """Enhanced name extraction that handles various patterns"""
    # Patterns for customer operations
    customer_patterns = [
        r'delete\s+customer\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'remove\s+customer\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'update\s+customer\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'delete\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'remove\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)'
    ]

    # Patterns for product operations
    product_patterns = [
        r'delete\s+product\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'remove\s+product\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'update\s+(?:price\s+of\s+)?([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'change\s+price\s+of\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'(?:price\s+of\s+)([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+(?:to|=)'
    ]

    all_patterns = customer_patterns + product_patterns

    for pattern in all_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


def extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+', text)
    return match.group(0) if match else None


def extract_price(text):
    # Look for price patterns like "to 25", "= 30.50", "$15.99"
    price_patterns = [
        r'to\s+\$?(\d+(?:\.\d+)?)',
        r'=\s+\$?(\d+(?:\.\d+)?)',
        r'\$(\d+(?:\.\d+)?)',
        r'(\d+(?:\.\d+)?)\s*dollars?'
    ]

    for pattern in price_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1))

    return None


def generate_table_description(df: pd.DataFrame, content: dict, action: str, tool: str) -> str:
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

    user_prompt = f"""
    Analyze this table data and generate a single insightful line about it:

    Context: {json.dumps(context, indent=2)}

    Generate one line describing what this data represents and any key insights.
    """

    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        response = groq_client.invoke(messages)
        return response.content.strip()
    except Exception as e:
        return f"Retrieved {len(df)} records from the database."


# ========== MAIN ==========
if application == "MCP Application":
    user_avatar_url = "https://cdn-icons-png.flaticon.com/512/1946/1946429.png"
    agent_avatar_url = "https://cdn-icons-png.flaticon.com/512/4712/4712039.png"

    MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
    st.session_state["MCP_SERVER_URL"] = MCP_SERVER_URL

    # Discover tools dynamically if not already done
    if not st.session_state.available_tools:
        with st.spinner("Discovering available tools..."):
            discovered_tools = discover_tools()
            st.session_state.available_tools = discovered_tools
            st.session_state.tool_states = {tool: True for tool in discovered_tools.keys()}

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
        if st.button("🔄 Activate Connection", key="refresh_tools_main", help="Rediscover available tools"):
            with st.spinner("Refreshing tools..."):
                MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
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

            # Generate LLM response for the operation
            llm_response = generate_llm_response(content, action, tool, user_query)

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
                        updated_df = pd.DataFrame(updated_table["result"])
                        st.table(updated_df)
                except Exception as fetch_err:
                    st.info(f"Could not retrieve updated table: {fetch_err}")

            if action == "read" and isinstance(content["result"], list):
                st.markdown("#### Here's the current table:")
                df = pd.DataFrame(content["result"])
                st.table(df)

                # Enhanced descriptions for new features
                if tool == "sales_crud" and len(df.columns) > 0:
                    # Check for column filtering
                    if len(df.columns) < 7:  # If fewer columns than default, column filtering was likely used
                        st.info(
                            f"📊 Column filtering applied - Showing {len(df.columns)} selected columns: {', '.join(df.columns)}")

                    # Check for WHERE clause filtering
                    if "where_condition" in msg.get("request", {}).get("args", {}):
                        where_cond = msg["request"]["args"]["where_condition"]
                        st.info(f"🔍 Data filtered with WHERE clause: {where_cond}")

                    # Check for different ETL formats based on column names
                    if "sale_summary" in df.columns:
                        st.info("📊 Data formatted with String Concatenation - Combined fields for readability")
                    elif "sale_date" in df.columns and isinstance(df["sale_date"].iloc[0] if len(df) > 0 else None,
                                                                  str):
                        st.info("📅 Data formatted with Data Format Conversion - Dates converted to string format")
                    elif any(
                            "." in str(val) and len(str(val).split(".")[-1]) == 2 for val in df.get("unit_price", []) if
                            pd.notna(val)):
                        st.info("💰 Data formatted with Decimal Value Formatting - Prices formatted to 2 decimal places")
                    else:
                        st.markdown(f"The table contains {len(df)} sales records with cross-database information.")
                elif tool == "sqlserver_crud":
                    st.markdown(
                        f"The table contains {len(df)} customers with their respective IDs, names, emails, and creation timestamps.")
                elif tool == "postgresql_crud":
                    st.markdown(
                        f"The table contains {len(df)} products with their respective IDs, names, prices, and descriptions.")
                else:
                    st.markdown(f"The table contains {len(df)} records.")

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
                "Chat Input",  # Provide a label
                placeholder="How can I help you today?",
                label_visibility="collapsed",  # Hide the label visually
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

    # ========== HANDLE HAMBURGER ==========
    if hamburger_clicked:
        st.session_state["show_menu"] = not st.session_state.get("show_menu", False)
        st.rerun()

    # ========== PROCESS CHAT INPUT ==========
    if send_clicked and user_query_input:
        user_query = user_query_input
        user_steps = []
        try:
            enabled_tools = [k for k, v in st.session_state.tool_states.items() if v]
            if not enabled_tools:
                raise Exception("No tools are enabled. Please enable at least one tool in the menu.")

            p = parse_user_query(user_query, st.session_state.available_tools)
            tool = p.get("tool")
            if tool not in enabled_tools:
                raise Exception(f"Tool '{tool}' is disabled. Please enable it in the menu.")
            if tool not in st.session_state.available_tools:
                raise Exception(
                    f"Tool '{tool}' is not available. Available tools: {', '.join(st.session_state.available_tools.keys())}")

            action = p.get("action")
            args = p.get("args", {})

            # VALIDATE AND CLEAN PARAMETERS
            args = validate_and_clean_parameters(tool, args)
            args = normalize_args(args)
            p["args"] = args

            # ========== ENHANCED NAME-BASED RESOLUTION ==========

            # For SQL Server (customers) operations
            if tool == "sqlserver_crud":
                if action in ["update", "delete"] and "name" in args and "customer_id" not in args:
                    # First, try to find the customer by name
                    name_to_find = args["name"]
                    try:
                        # Search for customer by name
                        read_result = call_mcp_tool(tool, "read", {})
                        if isinstance(read_result, dict) and "result" in read_result:
                            customers = read_result["result"]
                            # Try exact match first
                            exact_matches = [c for c in customers if c.get("Name", "").lower() == name_to_find.lower()]
                            if exact_matches:
                                args["customer_id"] = exact_matches[0]["Id"]
                            else:
                                # Try partial matches (first name or last name)
                                partial_matches = [c for c in customers if
                                                   name_to_find.lower() in c.get("Name", "").lower() or
                                                   name_to_find.lower() in c.get("FirstName", "").lower() or
                                                   name_to_find.lower() in c.get("LastName", "").lower()]
                                if partial_matches:
                                    args["customer_id"] = partial_matches[0]["Id"]
                                else:
                                    raise Exception(f"❌ Customer '{name_to_find}' not found")
                    except Exception as e:
                        if "not found" in str(e):
                            raise e
                        else:
                            raise Exception(f"❌ Error finding customer '{name_to_find}': {str(e)}")

                # Extract new email for updates
                if action == "update" and "new_email" not in args:
                    possible_email = extract_email(user_query)
                    if possible_email:
                        args["new_email"] = possible_email

            # For PostgreSQL (products) operations
            elif tool == "postgresql_crud":
                if action in ["update", "delete"] and "name" in args and "product_id" not in args:
                    # First, try to find the product by name
                    name_to_find = args["name"]
                    try:
                        # Search for product by name
                        read_result = call_mcp_tool(tool, "read", {})
                        if isinstance(read_result, dict) and "result" in read_result:
                            products = read_result["result"]
                            # Try exact match first
                            exact_matches = [p for p in products if p.get("name", "").lower() == name_to_find.lower()]
                            if exact_matches:
                                args["product_id"] = exact_matches[0]["id"]
                            else:
                                # Try partial matches
                                partial_matches = [p for p in products if
                                                   name_to_find.lower() in p.get("name", "").lower()]
                                if partial_matches:
                                    args["product_id"] = partial_matches[0]["id"]
                                else:
                                    raise Exception(f"❌ Product '{name_to_find}' not found")
                    except Exception as e:
                        if "not found" in str(e):
                            raise e
                        else:
                            raise Exception(f"❌ Error finding product '{name_to_find}': {str(e)}")

                # Extract new price for updates
                if action == "update" and "new_price" not in args:
                    possible_price = extract_price(user_query)
                    if possible_price is not None:
                        args['new_price'] = possible_price

            # Update the parsed args
            p["args"] = args

            # Handle describe operations
            if action == "describe" and "table_name" in args:
                if tool == "sqlserver_crud" and args["table_name"].lower() in ["customer", "customer table"]:
                    args["table_name"] = "Customers"
                if tool == "postgresql_crud" and args["table_name"].lower() in ["product", "product table"]:
                    args["table_name"] = "products"

            raw = call_mcp_tool(p["tool"], p["action"], p.get("args", {}))
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
        else:
            st.session_state.messages.append({
                "role": "user",
                "content": user_query,
                "format": "text",
            })
            for step in user_steps:
                st.session_state.messages.append(step)
            if isinstance(raw, dict) and "sql" in raw and "result" in raw:
                reply, fmt = raw, "sql_crud"
            else:
                reply, fmt = format_natural(raw), "text"
            assistant_message = {
                "role": "assistant",
                "content": reply,
                "format": fmt,
                "request": p,
                "tool": p.get("tool"),
                "action": p.get("action"),
                "args": p.get("args"),
                "user_query": user_query,  # Added user_query to the message
            }
            st.session_state.messages.append(assistant_message)
        st.rerun()  # Rerun so chat output appears

    # ========== 4. AUTO-SCROLL TO BOTTOM ==========
    components.html("""
        <script>
          setTimeout(() => { window.scrollTo(0, document.body.scrollHeight); }, 80);
        </script>
    """)

# ========== ETL EXAMPLES HELP SECTION ==========
with st.expander("🔧 Enhanced Features & Examples"):
    st.markdown("""
    ### NEW: Column Filtering & WHERE Clause Support

    #### Column Filtering
    Select specific columns to display in your results:
    - **"show only product name and quantity"** - Display selected columns only
    - **"display customer names and prices"** - Filter to specific data
    - **Available columns**: sale_id, customer_first_name, customer_last_name, product_name, product_description, quantity, unit_price, total_price, sale_date, customer_email

    #### WHERE Clause Filtering
    Filter data with SQL-like conditions:
    - **"sales where price > 14"** - Show sales with unit price above $14
    - **"show sales where quantity >= 2"** - Multi-item purchases only
    - **"display sales for Alice"** - Filter by customer name
    - **"sales where customer_id = 1"** - Filter by customer ID

    #### Bulk Operations with WHERE
    Perform operations on multiple records:
    - **"update sales where price > 14 set quantity to 5"** - Bulk update quantities
    - **"delete sales where quantity = 1"** - Remove single-item purchases
    - **"update price to 20 where product is Widget"** - Update specific product prices

    #### Combined Features
    Use both column filtering and WHERE clauses together:
    - **"show customer names and prices where total > 20"** - High-value sales with specific columns
    - **"display product and quantity where customer is Alice"** - Customer-specific filtered data

    ### ETL Display Formatting Functions

    #### 1. Data Format Conversion
    - **Query Examples:** 
      - "show sales with data format conversion"
      - "convert sales data format"
      - "format sales data for export"
    - **What it does:** Converts dates to string format, removes unnecessary fields

    #### 2. Decimal Value Formatting  
    - **Query Examples:**
      - "format sales prices with decimal formatting" 
      - "show sales with 2 decimal places"
      - "decimal value formatting for sales"
    - **What it does:** Formats all prices to exactly 2 decimal places as strings

    #### 3. String Concatenation
    - **Query Examples:**
      - "combine sales fields for readability"
      - "show sales with concatenated fields"
    - **What it does:** Creates readable summary fields by combining related data

    #### 4. Null Value Removal/Handling
    - **Query Examples:**
      - "clean sales data with null handling"
      - "remove nulls from sales data"
      - "handle null values in sales"
    - **What it does:** Filters out incomplete records and handles null values

    ### Regular Operations
    - **"list all sales"** - Shows regular unformatted sales data
    - **"show customers"** - Shows customer data
    - **"list products"** - Shows product inventory

    ### Smart Name-Based Operations
    - **"delete customer Alice"** - Finds and deletes Alice by name
    - **"delete Alice Johnson"** - Finds customer by full name
    - **"remove Johnson"** - Finds customer by last name
    - **"delete product Widget"** - Finds and deletes Widget by name
    - **"update price of Gadget to 25"** - Updates Gadget price to $25
    - **"change email of Bob to bob@new.com"** - Updates Bob's email
    """)



