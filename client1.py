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
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import hashlib

load_dotenv()

# Initialize Groq client with environment variable
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("🔐 GROQ_API_KEY environment variable is not set. Please add it to your environment.")
    st.stop()

groq_client = Groq(
    api_key=GROQ_API_KEY,
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
        padding: 24px;
        background-color: rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        margin: 12px;
    }
    .sidebar-block h3 {
        margin-top: 0;
        color: #fff;
    }
    .sidebar-block p, .sidebar-block ul {
        color: rgba(255, 255, 255, 0.8);
    }
    .sidebar-block ul {
        padding-left: 20px;
    }
    .sidebar-block ul li {
        margin-bottom: 8px;
    }
    .stChatFloatingBottom {
        padding-bottom: 2rem;
    }
    .st-emotion-cache-1c99sb8 {
        gap: 0px;
    }
    .st-emotion-cache-1830500 {
        padding: 0px 24px;
    }
    .main-header {
        text-align: center;
        font-size: 2.5rem;
        font-weight: bold;
        color: #0d47a1;
        margin-bottom: 1rem;
    }
    .stChatMessage {
        background-color: #f0f4f8;
        padding: 1rem;
        border-radius: 12px;
        margin-bottom: 1rem;
    }
    .stChatMessage.st-ai {
        background-color: #e3f2fd;
    }
    .sql-block {
        font-family: monospace;
        background-color: #e0e0e0;
        padding: 8px;
        border-radius: 6px;
        margin-top: 8px;
        white-space: pre-wrap;
        word-wrap: break-word;
    }
    .markdown-container {
        padding: 1rem;
        background-color: #f0f4f8;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    </style>
""", unsafe_allow_html=True)


# ========== HELPERS & FORMATTERS ==========
def convert_df_to_base64_image(df: pd.DataFrame):
    """
    Converts a pandas DataFrame to a base64 encoded image (PNG).
    """
    # This is a placeholder as direct HTML to image conversion in a simple Python script is complex.
    # In a real-world app, you might use a service or a library like `html2image` or `imgkit`.
    st.warning("Cannot convert table to image on this platform. Displaying as a DataFrame.")
    return None

def format_natural(raw: dict, selected_display_option: str) -> any:
    """
    Formats the raw dictionary response from the MCP server into a human-readable format.
    """
    if selected_display_option == "Natural language":
        if "result" in raw:
            result = raw["result"]
            if isinstance(result, list):
                if not result:
                    return "No results found."
                
                # Check if the list contains dictionaries
                if all(isinstance(item, dict) for item in result):
                    df = pd.DataFrame(result)
                    return df.to_markdown(index=False)
                else:
                    return f"Result: {result}"
            elif isinstance(result, dict):
                # Simple markdown for a single dictionary
                return "\n".join([f"**{k}**: {v}" for k, v in result.items()])
            else:
                return str(result)
        else:
            return raw
    else:
        return raw

def format_sql_crud(raw: dict) -> dict:
    """
    Formats the raw dictionary response from the CRUD tool for structured display.
    """
    reply_content = {}
    sql = raw.get("sql")
    if sql:
        reply_content["sql"] = f"```sql\n{sql}\n```"
    
    result = raw.get("result")
    if isinstance(result, list):
        if all(isinstance(item, dict) for item in result):
            reply_content["result"] = pd.DataFrame(result)
        else:
            reply_content["result"] = f"Result: {result}"
    elif isinstance(result, dict):
        reply_content["result"] = pd.DataFrame([result])
    else:
        reply_content["result"] = str(result)
        
    return reply_content


# ========== MCP TOOL CALLING UTILITIES ==========
async def call_mcp_tool(tool_name: str, **kwargs: any) -> any:
    """
    Asynchronously calls a tool on the MCP server and handles streaming.
    """
    try:
        raw_response = ""
        async for chunk in client.stream_tool(tool_name, **kwargs):
            if chunk.type == "text":
                raw_response += chunk.content
            
        try:
            response_data = json.loads(raw_response)
        except json.JSONDecodeError:
            response_data = {"result": raw_response}
        
        return response_data

    except Exception as e:
        return {"result": f"❌ Error calling tool '{tool_name}': {e}"}


async def process_user_query(prompt: str):
    """
    Processes the user's query by deciding which tool to call.
    """
    messages = [
        {"role": "system", "content": """
                You are an AI assistant for database management. You can perform CRUD operations,
                generate visualizations, and check database health.
                Your tools are `sql_crud_tool`, `analyze_and_visualize_tool`, and `health_check_tool`.
                Use `sql_crud_tool` for all data manipulation.
                Use `analyze_and_visualize_tool` for all charts and reports.
                Use `health_check_tool` for status checks.
        """},
        {"role": "user", "content": prompt},
    ]

    try:
        chat_completion = groq_client.chat.completions.with_options(tool_choice="auto").create(
            messages=messages,
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "sql_crud_tool",
                        "description": "Performs all CRUD operations on the databases (MySQL and PostgreSQL).",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "operation": {"type": "string", "enum": ["select", "insert", "update", "delete", "describe", "list_tables"]},
                                "database": {"type": "string", "enum": ["mysql", "postgres_products", "postgres_sales"]},
                                "table_name": {"type": "string"},
                                "columns": {"type": "string"},
                                "where_clause": {"type": "string"},
                                "data": {"type": "object"},
                                "limit": {"type": "integer"}
                            },
                            "required": ["operation", "database", "table_name"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "analyze_and_visualize_tool",
                        "description": "Generates data visualizations or detailed reports based on user queries.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "user_query": {"type": "string"},
                                "database": {"type": "string", "enum": ["mysql", "postgres_products", "postgres_sales"]}
                            },
                            "required": ["user_query", "database"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "health_check_tool",
                        "description": "Checks the health and connectivity of the backend databases.",
                        "parameters": {"type": "object", "properties": {}}
                    }
                },
            ]
        )
        response_message = chat_completion.choices[0].message
        
        if response_message.tool_calls:
            tool_call = response_message.tool_calls[0]
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)
            
            tool_call_message = {
                "role": "tool_call",
                "content": {"name": tool_name, "args": tool_args}
            }
            st.session_state.messages.append(tool_call_message)

            raw = await call_mcp_tool(tool_name, **tool_args)
            reply_content, fmt = format_sql_crud(raw), "sql_crud"

            assistant_message = {
                "role": "assistant",
                "content": reply_content,
                "format": fmt,
                "request": tool_call,
                "tool": tool_name,
                "action": "run_tool",
                "args": tool_args,
                "user_query": prompt,
            }
            st.session_state.messages.append(assistant_message)
        else:
            reply_content = {"result": response_message.content}
            fmt = "text"
            assistant_message = {
                "role": "assistant",
                "content": reply_content,
                "format": fmt,
                "request": {"tool_calls": []},
                "tool": None,
                "action": "text_response",
                "args": None,
                "user_query": prompt,
            }
            st.session_state.messages.append(assistant_message)

    except Exception as e:
        error_message = f"❌ An error occurred: {e}"
        st.error(error_message)
        st.session_state.messages.append({"role": "error", "content": {"result": error_message}})


# ========== STREAMLIT APP LAYOUT & LOGIC ==========
def display_message(message):
    """Displays a single chat message based on its role and format."""
    with st.chat_message(message["role"]):
        if message["format"] == "sql_crud":
            if "sql" in message["content"]:
                st.markdown("### SQL Query:")
                st.code(message["content"]["sql"], language="sql")
            st.markdown("### Result:")
            result = message["content"]["result"]
            if isinstance(result, pd.DataFrame):
                for col in result.columns:
                    if pd.api.types.is_numeric_dtype(result[col]):
                        result[col] = result[col].apply(lambda x: f'{x:.2f}' if isinstance(x, (float, Decimal)) else x)
                st.dataframe(result, hide_index=True)
            elif isinstance(result, dict):
                st.json(result)
            else:
                st.write(result)
        elif message["format"] == "visualization":
            try:
                raw_dict = ast.literal_eval(message["content"])
                fig_json = raw_dict.get("plotly_json")
                if fig_json:
                    fig = go.Figure(json.loads(fig_json))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.json(message["content"])
            except (ValueError, SyntaxError) as e:
                st.warning(f"Failed to parse visualization data: {e}. Displaying raw content.")
                st.write(message["content"])
        else:
            if message["role"] == "tool_call":
                st.markdown(f"**Tool Call:** `{message['content']['name']}`")
                st.json(message["content"]["args"])
            elif message["role"] == "assistant":
                if isinstance(message["content"]["result"], str):
                    st.markdown(message["content"]["result"])
                else:
                    st.json(message["content"]["result"])
            else:
                if isinstance(message["content"], str):
                    st.markdown(message["content"])
                else:
                    st.json(message["content"])


def main():
    """Main application logic"""

    st.title("MCP CRUD Chat 💬")
    st.markdown("Talk to your databases using natural language!")

    # Display the available tools in the sidebar
    with st.sidebar:
        st.markdown("<div class='sidebar-title'>MCP CRUD Chat</div>", unsafe_allow_html=True)
        st.image("https://placehold.co/300x150/4286f4/ffffff?text=Database+Visualizer", use_column_width=True)

        st.markdown(
            """
            ### 🛠️ Key Features
            - **Natural Language** Queries
            - **Visualize** Data with Charts
            - **Full CRUD** Support
            - **Database Health** Checks
            """
        )
        st.image("https://placehold.co/300x150/4286f4/ffffff?text=Real-time+Chat", use_column_width=True)

        st.markdown(
            """
            ### 🎯 Pro Tips for Visualizations
            1. **Be Specific**: "bar chart of sales by customer" works better than "chart sales"
            2. **Use Data Context**: The system auto-detects the best data source based on your query
            3. **Combine with Filters**: "bar chart of sales above $50"
            4. **Multiple Views**: Use "dashboard" for comprehensive multi-chart analysis
            5. **Quick Access**: Use the visualization buttons that appear after any data query
            
            ### 🔍 AI-Powered Insights
            Every visualization includes:
            - **Automatic trend analysis**
            - **Key pattern detection**
            - **Business insights and recommendations**
            - **Export options for charts**
            - **Interactive drill-down capabilities**
            """
        )

    # Corrected client instantiation: We now use MCP_SERVER_URL and StreamableHttpTransport.
    try:
        MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://127.0.0.1:8000")
        client = Client(base_url=f"{MCP_SERVER_URL}/api", transport=StreamableHttpTransport)
    except Exception as e:
        st.error(f"❌ Failed to connect to MCP Server: {e}")
        st.stop()

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
        
    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        display_message(message)

    # Accept user input
    if prompt := st.chat_input("What do you need help with?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        # Display user message in chat message container
        display_message({"role": "user", "content": prompt})
        
        # Process the query asynchronously
        asyncio.run(process_user_query(prompt))

    # Auto-scroll to the bottom of the page
    components.html("""
        <script>
            setTimeout(() => {
                window.scrollTo(0, document.body.scrollHeight);
            }, 80);
        </script>
    """)

if __name__ == "__main__":
    main()
