#!/usr/bin/env python3
"""
⚾ Baseball Dugout - Player Analysis Agent

Analyzes baseball players using custom MCP tools via Arcade gateway.
Data comes from Snowflake - no model knowledge used.
"""

import asyncio
import io
import logging
import os
import re
import sys
import warnings
from pathlib import Path

from dotenv import load_dotenv
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")

# Suppress noisy logs
for logger in ["langchain_mcp_adapters", "httpx", "httpcore", "mcp"]:
    logging.getLogger(logger).setLevel(logging.ERROR)
warnings.filterwarnings("ignore")


class StderrFilter(io.StringIO):
    """Filter noisy stderr messages."""
    
    SUPPRESS = ["Session termination failed", "LangGraphDeprecated"]
    
    def __init__(self, original):
        super().__init__()
        self.original = original
    
    def write(self, msg):
        if not any(p in msg for p in self.SUPPRESS):
            self.original.write(msg)
        return len(msg)
    
    def flush(self):
        self.original.flush()


LOGO = """
\033[33m
    ██████╗  █████╗ ███████╗███████╗██████╗  █████╗ ██╗     ██╗     
    ██╔══██╗██╔══██╗██╔════╝██╔════╝██╔══██╗██╔══██╗██║     ██║     
    ██████╔╝███████║███████╗█████╗  ██████╔╝███████║██║     ██║     
    ██╔══██╗██╔══██║╚════██║██╔══╝  ██╔══██╗██╔══██║██║     ██║     
    ██████╔╝██║  ██║███████║███████╗██████╔╝██║  ██║███████╗███████╗
    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝
                                                                     
    ██████╗ ██╗   ██╗ ██████╗  ██████╗ ██╗   ██╗████████╗           
    ██╔══██╗██║   ██║██╔════╝ ██╔═══██╗██║   ██║╚══██╔══╝           
    ██║  ██║██║   ██║██║  ███╗██║   ██║██║   ██║   ██║              
    ██║  ██║██║   ██║██║   ██║██║   ██║██║   ██║   ██║              
    ██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝   ██║              
    ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝    ╚═╝              
\033[0m
\033[90m    ───────────────────────────────────────────────────────────
    ⚾  Your AI Scout  •  Powered by Arcade + Snowflake + LangChain
    ───────────────────────────────────────────────────────────\033[0m
"""

# Configuration from .env
MCP_SERVER_URL = os.getenv("ARCADE_MCP_GATEWAY")
ARCADE_API_KEY = os.getenv("ARCADE_API_KEY")
ARCADE_USER_ID = os.getenv("ARCADE_USER_EMAIL")

# Tools we want to use
ALLOWED_TOOLS = [
    "BaseballDugout_GetPlayerStats",
    "BaseballDugout_GetTeamStats", 
    "BaseballDugout_ComparePlayers",
    "BaseballDugout_GetSeasonLeaders",
    "BaseballDugout_ExecuteBaseballQuery",
    "GoogleDocs_CreateDocumentFromText",
    "Gmail_SendEmail",
]


def extract_auth_url(text: str) -> str | None:
    """Extract OAuth URL from response text."""
    pattern = r'https://accounts\.google\.com/o/oauth2[^\s\)\]\"\'<>]+'
    matches = re.findall(pattern, text)
    if matches:
        return matches[0]
    
    pattern = r'https://[^\s\)\]\"\'<>]*oauth[^\s\)\]\"\'<>]*'
    matches = re.findall(pattern, text, re.IGNORECASE)
    if matches:
        return matches[0]
    
    return None


def print_step(step: str, status: str = "running"):
    """Print a formatted step."""
    icons = {
        "running": "\033[33m⏳\033[0m",
        "done": "\033[32m✓\033[0m",
        "error": "\033[31m✗\033[0m",
        "auth": "\033[35m🔐\033[0m",
    }
    icon = icons.get(status, "•")
    print(f"    {icon} {step}")


async def analyze_player(player_name: str, recipient_email: str) -> str:
    """Connect to Arcade MCP Gateway and analyze a baseball player."""
    
    # Validate config
    if not all([MCP_SERVER_URL, ARCADE_API_KEY, ARCADE_USER_ID, os.getenv("OPENAI_API_KEY")]):
        sys.exit("\n\033[31mError: Missing required environment variables in .env\033[0m")
    
    print(f"\n\033[90m    Connecting to Arcade MCP Gateway...\033[0m")
    
    # Connect to Arcade MCP Gateway
    client = MultiServerMCPClient({
        "arcade": {
            "url": MCP_SERVER_URL,
            "transport": "streamable_http",
            "headers": {
                "Authorization": f"Bearer {ARCADE_API_KEY}",
                "Arcade-User-ID": ARCADE_USER_ID,
            },
        }
    })
    
    # Get and filter tools
    all_tools = await client.get_tools()
    tools = [t for t in all_tools if t.name in ALLOWED_TOOLS]
    
    print(f"\033[90m    Tools loaded: {len(tools)}\033[0m\n")
    
    # Create agent
    agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools)
    
    # Build the query
    query = f"""You are a baseball data analyst. ONLY use the provided tools to get information.
DO NOT use any prior knowledge - ALL data must come from tool calls.

Task: Research and share analysis for baseball player "{player_name}"

Follow these steps IN ORDER:

1. RESEARCH: Use BaseballDugout_GetPlayerStats with player_name "{player_name}" to get their career stats

2. CREATE DOCUMENT: Use GoogleDocs_CreateDocumentFromText to create a document with:
   - title: "Baseball Scouting Report: {player_name}"  
   - text_content: A nicely formatted report with all their stats, career highlights, and key achievements

3. SEND EMAIL: Use Gmail_SendEmail to share the document:
   - recipient: "{recipient_email}"
   - subject: "Baseball Scouting Report: {player_name}"
   - body: A brief message saying you've completed the analysis with a link to the Google Doc

4. Return a summary of what was done and the Google Doc URL.

If player not found, say "Player not found in database."
"""
    
    print("\033[1m    WORKFLOW PROGRESS\033[0m")
    print("    " + "─" * 50)
    
    max_retries = 3
    for attempt in range(max_retries):
        full_response = ""
        current_tool = ""
        
        async for event in agent.astream_events(
            {"messages": [{"role": "user", "content": query}]},
            version="v2"
        ):
            kind = event["event"]
            
            if kind == "on_tool_start":
                tool_name = event['name']
                if "GetPlayerStats" in tool_name:
                    current_tool = "Researching player stats from Snowflake..."
                elif "CreateDocument" in tool_name:
                    current_tool = "Creating Google Doc with scouting report..."
                elif "SendEmail" in tool_name:
                    current_tool = f"Sending email to {recipient_email}..."
                else:
                    current_tool = f"Running {tool_name}..."
                print_step(current_tool, "running")
                
            elif kind == "on_tool_end":
                print_step(current_tool.replace("...", ""), "done")
                
            elif kind == "on_chat_model_stream":
                chunk = event.get("data", {}).get("chunk")
                if chunk and hasattr(chunk, "content") and chunk.content:
                    full_response += chunk.content
        
        # Check if auth is required
        auth_url = extract_auth_url(full_response)
        if auth_url and "authorize" in full_response.lower():
            print()
            print("    " + "─" * 50)
            print_step("Authorization required for Google services", "auth")
            print()
            print(f"\033[36m    Please visit this URL to authorize:\033[0m")
            print(f"\n    \033[4m{auth_url}\033[0m\n")
            input("    \033[90mPress Enter after completing authorization...\033[0m")
            print()
            print("    \033[33m♻️  Retrying workflow...\033[0m\n")
            print("\033[1m    WORKFLOW PROGRESS\033[0m")
            print("    " + "─" * 50)
            continue
        
        # Success
        print("    " + "─" * 50)
        return full_response
    
    return "Max retries reached. Please try again."


async def main() -> None:
    """Entry point."""
    # Filter out noisy stderr messages
    sys.stderr = StderrFilter(sys.__stderr__)
    
    # Clear screen and show logo
    print("\033[2J\033[H")
    print(LOGO)
    
    # Get player name
    print("\n\033[1m    WHO DO YOU WANT TO SCOUT?\033[0m")
    print("    " + "─" * 50)
    player_name = input("    Enter player name (e.g., Babe Ruth): \033[36m").strip()
    print("\033[0m", end="")
    
    if not player_name:
        print("\n    \033[31mNo player name provided. Exiting.\033[0m\n")
        return
    
    # Get email
    print()
    print("\033[1m    WHO SHOULD RECEIVE THE REPORT?\033[0m")
    print("    " + "─" * 50)
    recipient_email = input("    Enter email address: \033[36m").strip()
    print("\033[0m", end="")
    
    if not recipient_email:
        print("\n    \033[31mNo email provided. Exiting.\033[0m\n")
        return
    
    result = await analyze_player(player_name, recipient_email)
    
    print()
    print("\033[1m\033[32m    ✓ SCOUTING COMPLETE\033[0m")
    print("    " + "─" * 50)
    
    # Extract and display the Google Doc URL if present
    doc_match = re.search(r'https://docs\.google\.com/document/d/[^\s\)\]\"\'<>]+', result)
    if doc_match:
        print(f"\n    \033[1mGoogle Doc:\033[0m")
        print(f"    \033[4m\033[36m{doc_match.group()}\033[0m")
    
    print(f"\n    \033[1mEmail sent to:\033[0m {recipient_email}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
