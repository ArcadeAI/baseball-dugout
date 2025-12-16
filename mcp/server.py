#!/usr/bin/env python3
"""
⚾ Baseball Dugout - Your AI Scout
Baseball Analytics MCP Server powered by Snowflake & Arcade

This MCP server provides baseball analytics tools backed by Snowflake.
Deploy to Arcade Cloud for secure, governed access to baseball data.
"""

import json
import re
import sys
from typing import Annotated, Optional

import httpx
from arcade_mcp_server import Context, MCPApp

# ============================================================================
# App Configuration
# ============================================================================

app = MCPApp(
    name="baseball_dugout",
    version="1.0.0",
    log_level="INFO",
)

# ============================================================================
# Input Validation
# ============================================================================

def sanitize_identifier(value: str) -> str:
    """Sanitize string input to prevent SQL injection."""
    if not value:
        return ""
    sanitized = re.sub(r"[^a-zA-Z0-9_\- ]", "", value)
    return sanitized[:100]


def validate_year(year: int) -> int:
    """Validate year is in reasonable baseball range."""
    if year < 1871 or year > 2030:
        raise ValueError(f"Year must be between 1871 and 2030, got {year}")
    return year


# ============================================================================
# Snowflake REST SQL API Client
# ============================================================================

class SnowflakeClient:
    """Lightweight Snowflake client using REST SQL API."""
    
    def __init__(self, account: str, user: str, password: str, 
                 warehouse: str = "BASEBALL_WAREHOUSE", 
                 database: str = "BASEBALL_ANALYTICS", 
                 schema: str = "PLAYER_DATA"):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self._token = None
    
    @property
    def base_url(self) -> str:
        account = self.account.replace("_", "-")
        return f"https://{account}.snowflakecomputing.com"
    
    def _get_token(self) -> str:
        if self._token:
            return self._token
        
        login_url = f"{self.base_url}/session/v1/login-request"
        payload = {
            "data": {
                "ACCOUNT_NAME": self.account.split(".")[0],
                "LOGIN_NAME": self.user,
                "PASSWORD": self.password,
                "CLIENT_APP_ID": "BaseballDugout",
                "CLIENT_APP_VERSION": "1.0.0",
            }
        }
        
        with httpx.Client(timeout=30.0) as client:
            response = client.post(login_url, json=payload, headers={"Content-Type": "application/json"})
            
            if response.status_code >= 400:
                raise RuntimeError(f"Snowflake login HTTP error {response.status_code}: {response.text[:300]}")
            
            data = response.json()
            
            if not data.get("success"):
                raise RuntimeError(f"Snowflake login failed: {data.get('message', 'Unknown error')}")
            
            self._token = data["data"]["token"]
            return self._token
    
    def execute_query(self, sql: str) -> list[dict]:
        token = self._get_token()
        sql_url = f"{self.base_url}/queries"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Snowflake Token=\"{token}\"",
            "Accept": "application/json",
        }
        
        with httpx.Client(timeout=60.0) as client:
            response = client.post(sql_url, json={"sqlText": sql}, headers=headers)
            
            if response.status_code >= 400:
                error_detail = response.text[:500]
                raise RuntimeError(f"Snowflake API error {response.status_code}: {error_detail}")
            
            data = response.json()
            if not data.get("success"):
                raise RuntimeError(f"Snowflake query failed: {data.get('message', 'Unknown error')}")
            
            return self._parse_response(data)
    
    def _parse_response(self, data: dict) -> list[dict]:
        result_data = data.get("data", {})
        row_type = result_data.get("rowtype", [])
        columns = [col.get("name", f"col_{i}") for i, col in enumerate(row_type)]
        rows = result_data.get("rowset", [])
        return [dict(zip(columns, row)) for row in rows]


def get_snowflake_client(context: Context) -> SnowflakeClient:
    """Create Snowflake client using secrets from Arcade context."""
    account = context.get_secret("SNOWFLAKE_ACCOUNT")
    user = context.get_secret("SNOWFLAKE_USER")
    password = context.get_secret("SNOWFLAKE_PASSWORD")
    
    if not all([account, user, password]):
        raise ValueError("Missing Snowflake credentials in Arcade secrets")
    
    return SnowflakeClient(
        account=account,
        user=user,
        password=password,
        warehouse="BASEBALL_WAREHOUSE",
        database="BASEBALL_ANALYTICS",
        schema="PLAYER_DATA"
    )


# ============================================================================
# Baseball Analytics Tools
# ============================================================================

@app.tool(requires_secrets=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])
def execute_baseball_query(
    context: Context,
    query: Annotated[str, "SQL query to execute against the baseball database"],
    explanation: Annotated[str, "Brief explanation of what this query is looking for"] = "",
) -> Annotated[str, "Query results in JSON format"]:
    """
    Execute a SQL query against the Baseball Analytics Snowflake database.
    
    Available tables: MASTER, BATTING, PITCHING, TEAMS, HALLOFFAME, ALLSTARFULL, SALARIES.
    
    IMPORTANT: Use fully qualified table names with BASEBALL_ANALYTICS.PLAYER_DATA prefix,
    e.g., BASEBALL_ANALYTICS.PLAYER_DATA.MASTER
    
    Key columns: player_id links players across tables, year_id for seasons.
    """
    client = get_snowflake_client(context)
    results = client.execute_query(query)
    return json.dumps(results, indent=2, default=str)


@app.tool(requires_secrets=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])
def get_player_stats(
    context: Context,
    player_name: Annotated[str, "Player name to search for (e.g., 'Babe Ruth', 'Ted Williams', 'David Ortiz')"],
) -> Annotated[str, "Career statistics for the player"]:
    """Get career batting and pitching statistics for a baseball player."""
    
    player_name = sanitize_identifier(player_name)
    client = get_snowflake_client(context)
    
    # Find player by name
    name_parts = player_name.split()
    if len(name_parts) >= 2:
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])
        name_filter = f"LOWER(name_first) LIKE LOWER('%{first_name}%') AND LOWER(name_last) LIKE LOWER('%{last_name}%')"
    else:
        name_filter = f"LOWER(name_last) LIKE LOWER('%{player_name}%') OR LOWER(name_first) LIKE LOWER('%{player_name}%')"
    
    # Get player info
    player_query = f"""
    SELECT player_id, name_first, name_last, birth_year, birth_country, 
           bats, throws, debut, final_game
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.MASTER 
    WHERE {name_filter}
    LIMIT 1
    """
    players = client.execute_query(player_query)
    
    if not players:
        return f"No player found matching: {player_name}"
    
    p = players[0]
    player_id = p.get('PLAYER_ID')
    
    # Get batting stats
    batting_query = f"""
    SELECT 
        COUNT(DISTINCT year_id) as seasons,
        SUM(g) as games,
        SUM(ab) as at_bats,
        SUM(h) as hits,
        SUM(hr) as home_runs,
        SUM(rbi) as rbis,
        SUM(r) as runs,
        SUM(bb) as walks,
        SUM(so) as strikeouts,
        SUM(sb) as stolen_bases,
        ROUND(SUM(h)::FLOAT / NULLIF(SUM(ab), 0), 3) as batting_avg
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.BATTING
    WHERE player_id = '{player_id}'
    """
    batting = client.execute_query(batting_query)
    b = batting[0] if batting else {}
    
    # Get pitching stats
    pitching_query = f"""
    SELECT 
        COUNT(DISTINCT year_id) as seasons,
        SUM(w) as wins,
        SUM(l) as losses,
        SUM(g) as games,
        SUM(gs) as starts,
        SUM(sv) as saves,
        SUM(so) as strikeouts,
        ROUND(AVG(era), 2) as career_era
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.PITCHING
    WHERE player_id = '{player_id}'
    """
    pitching = client.execute_query(pitching_query)
    pit = pitching[0] if pitching else {}
    
    # Get All-Star appearances
    allstar_query = f"""
    SELECT COUNT(*) as appearances
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.ALLSTARFULL
    WHERE player_id = '{player_id}'
    """
    allstar = client.execute_query(allstar_query)
    allstar_count = allstar[0].get('APPEARANCES', 0) if allstar else 0
    
    # Get Hall of Fame status
    hof_query = f"""
    SELECT inducted, year_id
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.HALLOFFAME
    WHERE player_id = '{player_id}' AND inducted = 'Y'
    LIMIT 1
    """
    hof = client.execute_query(hof_query)
    hof_status = f"Inducted {hof[0].get('YEAR_ID')}" if hof else "Not in Hall of Fame"
    
    output = f"""
# {p.get('NAME_FIRST', '')} {p.get('NAME_LAST', '')} ⚾

**Born:** {p.get('BIRTH_YEAR', 'Unknown')} ({p.get('BIRTH_COUNTRY', 'Unknown')})
**Bats/Throws:** {p.get('BATS', '?')}/{p.get('THROWS', '?')}
**Career:** {p.get('DEBUT', '?')} - {p.get('FINAL_GAME', '?')}
**All-Star Appearances:** {allstar_count}
**Hall of Fame:** {hof_status}
"""
    
    if b.get('GAMES') and int(b.get('GAMES') or 0) > 0:
        output += f"""
## Batting Statistics
| Stat | Value |
|------|-------|
| Seasons | {b.get('SEASONS', 0)} |
| Games | {b.get('GAMES', 0)} |
| At Bats | {b.get('AT_BATS', 0)} |
| Hits | {b.get('HITS', 0)} |
| Home Runs | {b.get('HOME_RUNS', 0)} |
| RBIs | {b.get('RBIS', 0)} |
| Batting Avg | {b.get('BATTING_AVG', '.000')} |
| Stolen Bases | {b.get('STOLEN_BASES', 0)} |
"""
    
    if pit.get('GAMES') and int(pit.get('GAMES') or 0) > 0:
        output += f"""
## Pitching Statistics
| Stat | Value |
|------|-------|
| Seasons | {pit.get('SEASONS', 0)} |
| W-L | {pit.get('WINS', 0)}-{pit.get('LOSSES', 0)} |
| Games | {pit.get('GAMES', 0)} |
| Starts | {pit.get('STARTS', 0)} |
| Saves | {pit.get('SAVES', 0)} |
| Strikeouts | {pit.get('STRIKEOUTS', 0)} |
| ERA | {pit.get('CAREER_ERA', '0.00')} |
"""
    
    return output


@app.tool(requires_secrets=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])
def get_team_stats(
    context: Context,
    team_name: Annotated[str, "Team name or ID (e.g., 'Yankees', 'NYA', 'Cubs', 'CHC')"],
    year: Annotated[Optional[int], "Optional: Filter to specific year"] = None,
) -> Annotated[str, "Team statistics and history"]:
    """Get statistics and history for a baseball team."""
    
    team_name = sanitize_identifier(team_name)
    client = get_snowflake_client(context)
    
    year_filter = f"AND t.year_id = {validate_year(year)}" if year else ""
    
    # Find team
    team_query = f"""
    SELECT t.team_id, t.name, tf.franch_name, MAX(t.year_id) as latest_year
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.TEAMS t
    LEFT JOIN BASEBALL_ANALYTICS.PLAYER_DATA.TEAMS_FRANCHISES tf ON t.franch_id = tf.franch_id
    WHERE LOWER(t.name) LIKE LOWER('%{team_name}%') 
       OR LOWER(t.team_id) = LOWER('{team_name}')
       OR LOWER(tf.franch_name) LIKE LOWER('%{team_name}%')
    GROUP BY t.team_id, t.name, tf.franch_name
    ORDER BY latest_year DESC
    LIMIT 1
    """
    teams = client.execute_query(team_query)
    
    if not teams:
        return f"No team found matching: {team_name}"
    
    team = teams[0]
    team_id = team.get('TEAM_ID')
    
    # Get team history
    stats_query = f"""
    SELECT 
        MIN(year_id) as first_year,
        MAX(year_id) as last_year,
        COUNT(*) as seasons,
        SUM(w) as total_wins,
        SUM(l) as total_losses,
        SUM(CASE WHEN ws_win = 'Y' THEN 1 ELSE 0 END) as world_series_wins,
        SUM(CASE WHEN lg_win = 'Y' THEN 1 ELSE 0 END) as pennants
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.TEAMS t
    WHERE t.team_id = '{team_id}' {year_filter}
    """
    stats = client.execute_query(stats_query)
    s = stats[0] if stats else {}
    
    total_wins = int(s.get('TOTAL_WINS') or 0)
    total_losses = int(s.get('TOTAL_LOSSES') or 0)
    win_pct = total_wins / (total_wins + total_losses) if (total_wins + total_losses) > 0 else 0
    
    output = f"""
# {team.get('NAME', team_name)} ⚾

**Franchise:** {team.get('FRANCH_NAME', 'Unknown')}
**Team ID:** {team_id}

## Franchise History
| Stat | Value |
|------|-------|
| First Season | {s.get('FIRST_YEAR', 'N/A')} |
| Most Recent | {s.get('LAST_YEAR', 'N/A')} |
| Total Seasons | {s.get('SEASONS', 0)} |
| All-Time Record | {total_wins}-{total_losses} (.{int(win_pct*1000):03d}) |
| World Series Titles | {s.get('WORLD_SERIES_WINS', 0)} 🏆 |
| Pennants | {s.get('PENNANTS', 0)} |
"""
    
    # Get recent seasons
    recent_query = f"""
    SELECT year_id, w, l, rank, ws_win, lg_win
    FROM BASEBALL_ANALYTICS.PLAYER_DATA.TEAMS
    WHERE team_id = '{team_id}'
    ORDER BY year_id DESC
    LIMIT 5
    """
    recent = client.execute_query(recent_query)
    
    if recent:
        output += "\n## Recent Seasons\n| Year | W | L | Rank | WS |\n|------|---|---|------|----|\n"
        for r in recent:
            ws = "🏆" if r.get('WS_WIN') == 'Y' else ""
            output += f"| {r.get('YEAR_ID')} | {r.get('W')} | {r.get('L')} | {r.get('RANK')} | {ws} |\n"
    
    return output


@app.tool(requires_secrets=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])
def compare_players(
    context: Context,
    player1_name: Annotated[str, "First player name (e.g., 'Ted Williams')"],
    player2_name: Annotated[str, "Second player name (e.g., 'Joe DiMaggio')"],
) -> Annotated[str, "Head-to-head comparison of two players"]:
    """Compare career statistics of two baseball players."""
    
    player1_name = sanitize_identifier(player1_name)
    player2_name = sanitize_identifier(player2_name)
    client = get_snowflake_client(context)
    
    def get_player_data(name: str) -> dict:
        name_parts = name.split()
        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])
            name_filter = f"LOWER(name_first) LIKE LOWER('%{first_name}%') AND LOWER(name_last) LIKE LOWER('%{last_name}%')"
        else:
            name_filter = f"LOWER(name_last) LIKE LOWER('%{name}%')"
        
        player_query = f"""
        SELECT m.player_id, m.name_first, m.name_last,
               SUM(b.g) as games, SUM(b.ab) as ab, SUM(b.h) as hits, 
               SUM(b.hr) as hr, SUM(b.rbi) as rbi,
               ROUND(SUM(b.h)::FLOAT / NULLIF(SUM(b.ab), 0), 3) as avg
        FROM BASEBALL_ANALYTICS.PLAYER_DATA.MASTER m
        LEFT JOIN BASEBALL_ANALYTICS.PLAYER_DATA.BATTING b ON m.player_id = b.player_id
        WHERE {name_filter}
        GROUP BY m.player_id, m.name_first, m.name_last
        LIMIT 1
        """
        result = client.execute_query(player_query)
        return result[0] if result else {}
    
    p1 = get_player_data(player1_name)
    p2 = get_player_data(player2_name)
    
    if not p1:
        return f"Player not found: {player1_name}"
    if not p2:
        return f"Player not found: {player2_name}"
    
    p1_name = f"{p1.get('NAME_FIRST', '')} {p1.get('NAME_LAST', '')}"
    p2_name = f"{p2.get('NAME_FIRST', '')} {p2.get('NAME_LAST', '')}"
    
    return f"""
# Head-to-Head: {p1_name} vs {p2_name} ⚾

| Stat | {p1_name} | {p2_name} |
|------|-----------|-----------|
| Games | {p1.get('GAMES', 0)} | {p2.get('GAMES', 0)} |
| At Bats | {p1.get('AB', 0)} | {p2.get('AB', 0)} |
| Hits | {p1.get('HITS', 0)} | {p2.get('HITS', 0)} |
| Home Runs | {p1.get('HR', 0)} | {p2.get('HR', 0)} |
| RBIs | {p1.get('RBI', 0)} | {p2.get('RBI', 0)} |
| Batting Avg | {p1.get('AVG', '.000')} | {p2.get('AVG', '.000')} |
"""


@app.tool(requires_secrets=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])
def get_season_leaders(
    context: Context,
    year: Annotated[int, "Season year (e.g., 2020)"],
    stat: Annotated[str, "Statistic to rank by: 'hr' (home runs), 'avg' (batting average), 'rbi', 'wins', 'era', 'strikeouts'"],
) -> Annotated[str, "League leaders for the specified stat and year"]:
    """Get the league leaders for a specific statistic in a given year."""
    
    year = validate_year(year)
    stat = sanitize_identifier(stat).lower()
    client = get_snowflake_client(context)
    
    stat_configs = {
        'hr': ('SUM(b.hr)', 'Home Runs', 'DESC', 'batting'),
        'avg': ('ROUND(SUM(b.h)::FLOAT / NULLIF(SUM(b.ab), 0), 3)', 'Batting Average', 'DESC', 'batting'),
        'rbi': ('SUM(b.rbi)', 'RBIs', 'DESC', 'batting'),
        'hits': ('SUM(b.h)', 'Hits', 'DESC', 'batting'),
        'wins': ('SUM(p.w)', 'Wins', 'DESC', 'pitching'),
        'era': ('ROUND(AVG(p.era), 2)', 'ERA', 'ASC', 'pitching'),
        'strikeouts': ('SUM(p.so)', 'Strikeouts', 'DESC', 'pitching'),
    }
    
    if stat not in stat_configs:
        return f"Unknown stat: {stat}. Available: hr, avg, rbi, hits, wins, era, strikeouts"
    
    calc, label, order, table = stat_configs[stat]
    
    if table == 'batting':
        query = f"""
        SELECT m.name_first || ' ' || m.name_last as player, 
               b.team_id as team,
               {calc} as value
        FROM BASEBALL_ANALYTICS.PLAYER_DATA.BATTING b
        JOIN BASEBALL_ANALYTICS.PLAYER_DATA.MASTER m ON b.player_id = m.player_id
        WHERE b.year_id = {year} AND b.ab >= 100
        GROUP BY m.player_id, m.name_first, m.name_last, b.team_id
        ORDER BY value {order}
        LIMIT 10
        """
    else:
        query = f"""
        SELECT m.name_first || ' ' || m.name_last as player,
               p.team_id as team,
               {calc} as value
        FROM BASEBALL_ANALYTICS.PLAYER_DATA.PITCHING p
        JOIN BASEBALL_ANALYTICS.PLAYER_DATA.MASTER m ON p.player_id = m.player_id
        WHERE p.year_id = {year} AND p.ip_outs >= 150
        GROUP BY m.player_id, m.name_first, m.name_last, p.team_id
        ORDER BY value {order}
        LIMIT 10
        """
    
    results = client.execute_query(query)
    
    output = f"# {year} {label} Leaders ⚾\n\n| Rank | Player | Team | {label} |\n|------|--------|------|--------|\n"
    for i, r in enumerate(results, 1):
        output += f"| {i} | {r.get('PLAYER', 'Unknown')} | {r.get('TEAM', '?')} | {r.get('VALUE', 0)} |\n"
    
    return output


if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"
    app.run(transport=transport, host="0.0.0.0", port=8000)

