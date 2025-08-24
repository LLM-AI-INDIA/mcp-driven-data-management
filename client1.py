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

groq_client = ChatGroq(
    groq_api_key=GROQ_API_KEY,
    model_name=os.environ.get("GROQ_MODEL", "moonshotai/kimi-k2-instruct")
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
    .sidebar-block {
        padding: 24px;
        padding-top: 0;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .sidebar-block h3 {
        color: #fff;
        margin-bottom: 12px;
        font-size: 1.2rem;
    }
    .sidebar-block ul {
        list-style-type: none;
        padding-left: 0;
    }
    .sidebar-block ul li {
        margin-bottom: 8px;
        font-size: 0.9rem;
    }
    .sidebar-block code {
        background-color: rgba(255,255,255,0.2);
        color: #fff;
        padding: 2px 6px;
        border-radius: 4px;
    }
    .chat-container {
        display: flex;
        flex-direction: column;
        height: 100vh;
    }
    .chat-messages {
        flex: 1;
        overflow-y: auto;
        padding-bottom: 80px;
    }
    .chat-input-box {
        position: fixed;
        bottom: 0;
        width: 100%;
        background: #f0f2f6;
        padding: 10px 0;
        z-index: 100;
        max-width: 900px;
        border-top: 1px solid #ddd;
    }
    .stTextInput>div>div>input {
        border-radius: 12px;
    }
    .stButton>button {
        width: 100%;
    }
    .message-container {
        display: flex;
        align-items: flex-start;
        margin-bottom: 20px;
    }
    .message-avatar {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        margin-right: 15px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.5rem;
        background-color: #f0f2f6;
    }
    .user-avatar {
        background-color: #007bff;
        color: white;
    }
    .ai-avatar {
        background-color: #4286f4;
        color: white;
    }
    .message-content {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 12px;
        border-top-left-radius: 0;
        max-width: 70%;
        word-wrap: break-word;
    }
    .user-content {
        background-color: #007bff;
        color: white;
        border-radius: 12px;
        border-top-right-radius: 0;
    }
    .message-sql {
        font-family: monospace;
        background-color: #e9e9e9;
        padding: 10px;
        border-radius: 8px;
        margin-top: 10px;
        overflow-x: auto;
    }
    .stDataFrame {
        width: 100%;
    }
    .stSpinner > div > span {
        font-size: 0.8rem;
    }
    </style>
""", unsafe_allow_html=True)


# ————————————————
# 1. MCP Client Setup
# ————————————————
# Initialize FastMCP client with streamable HTTP transport
client = Client(
    server_url=os.getenv("MCP_SERVER_URL", "http://localhost:8000"),
    transport=StreamableHttpTransport,
)


# ————————————————
# 2. Helper Functions
# ————————————————
def get_user_avatar():
    """Returns a simple emoji avatar for the user."""
    return "😃"

def get_ai_avatar():
    """Returns a simple emoji avatar for the assistant."""
    return "🧠"


def display_visual_analysis(analysis_data: dict):
    """
    Renders the textual analysis and an interactive chart from the server response.
    """
    # 1. Display the textual analysis
    st.markdown("---")
    st.markdown("### 📊 Data Analysis & Insights")
    st.markdown(analysis_data.get("analysis_text", "No analysis available."))
    st.markdown("---")

    # 2. Display the chart
    st.markdown("### 📈 Interactive Dashboard")
    try:
        df = pd.DataFrame(analysis_data.get("data", []))
        chart_spec = analysis_data.get("chart_spec", {})
        chart_type = chart_spec.get("chart_type")
        x_axis = chart_spec.get("x_axis")
        y_axis = chart_spec.get("y_axis")

        if df.empty or not x_axis or not y_axis:
            st.warning("⚠️ Not enough data to create a chart based on the analysis.")
            return

        fig = None
        if chart_type == "bar":
            fig = px.bar(df, x=x_axis, y=y_axis, title=f"Bar Chart of {y_axis.title()} by {x_axis.title()}")
        elif chart_type == "line":
            fig = px.line(df, x=x_axis, y=y_axis, title=f"Line Chart of {y_axis.title()} over {x_axis.title()}")
        elif chart_type == "scatter":
            fig = px.scatter(df, x=x_axis, y=y_axis, title=f"Scatter Plot of {x_axis.title()} vs {y_axis.title()}")
        elif chart_type == "pie":
            fig = px.pie(df, names=x_axis, values=y_axis, title=f"Pie Chart of {y_axis.title()}")
        else:
            st.warning(f"❌ Unsupported chart type from LLM: {chart_type}")
            return

        if fig:
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Failed to generate the interactive dashboard: {e}")

# ————————————————
# 3. Main Streamlit App
# ————————————————
# Initialize chat history in session state
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "Hello! I am a chat assistant for managing data in your databases. How can I help you today?",
            "format": "text",
        }
    ]

# Sidebar for controls and info
with st.sidebar:
    st.markdown("<h2 class='sidebar-title'>CRUD Chat</h2>", unsafe_allow_html=True)

    st.markdown("""
        <div class="sidebar-block">
            <h3>🎯 Supported Queries</h3>
            <ul>
                <li><code>list products</code></li>
                <li><code>show all careplans</code></li>
                <li><code>create product...</code></li>
                <li><code>update careplan...</code></li>
                <li><code>delete product...</code></li>
                <li><code>describe products table</code></li>
            </ul>
        </div>
        <div class="sidebar-block">
            <h3>📊 Advanced Visualizations</h3>
            <p>Try queries like:</p>
            <ul>
                <li><code>Analyze the products data.</code></li>
                <li><code>Show me insights from the careplan table.</code></li>
                <li><code>Create a dashboard for products.</code></li>
            </ul>
        </div>
    """, unsafe_allow_html=True)

# Main chat UI
st.title("MCP Database Chat")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(
        message["role"], avatar=get_ai_avatar() if message["role"] == "assistant" else get_user_avatar()
    ):
        if message["format"] == "sql_crud":
            content = message["content"]
            st.markdown(f"**Result:** {content['result']}")
            if content.get("sql"):
                st.code(content["sql"], language="sql")
        elif message["format"] == "visual_analysis":
            display_visual_analysis(message["content"])
        else:
            # Fallback for simple text messages
            st.markdown(message["content"])


# User input
if prompt := st.chat_input("What would you like to do?"):
    st.session_state.messages.append({"role": "user", "content": prompt, "format": "text"})
    
    # Display user message
    with st.chat_message("user", avatar=get_user_avatar()):
        st.markdown(prompt)

    with st.spinner("Thinking..."):
        try:
            # Simple keyword-based routing for analysis vs. standard ETL
            if any(k in prompt.lower() for k in ["analyze", "analysis", "insights", "dashboard"]):
                # Determine which database to analyze
                if "products" in prompt.lower():
                    database_to_analyze = "products"
                elif "careplan" in prompt.lower():
                    database_to_analyze = "careplan"
                else:
                    database_to_analyze = None # Let the LLM decide or fall back

                if database_to_analyze:
                    response = await client.run_tool("analyze_and_visualize_tool", database=database_to_analyze, user_query=prompt)
                    if response.get("status") == "success":
                        assistant_message = {
                            "role": "assistant",
                            "content": response,
                            "format": "visual_analysis",
                        }
                    else:
                        assistant_message = {
                            "role": "assistant",
                            "content": response.get("message", "An unexpected error occurred."),
                            "format": "text",
                        }
                else:
                    assistant_message = {
                        "role": "assistant",
                        "content": "Please specify which database you'd like to analyze (e.g., 'Analyze the **products** data').",
                        "format": "text",
                    }
            else:
                # Fallback to standard ETL tool for CRUD operations
                response = await client.run_tool("etl_tool", user_query=prompt)
                
                assistant_message = {
                    "role": "assistant",
                    "content": response,
                    "format": "sql_crud",
                    "sql": response.get("sql"),
                }
        except Exception as e:
            assistant_message = {
                "role": "assistant",
                "content": f"⚠️ An unexpected error occurred: {e}",
                "format": "text",
            }
        
        st.session_state.messages.append(assistant_message)
        st.experimental_rerun()
