import duckdb
import os
from datetime import datetime
from django.conf import settings
from django.shortcuts import render
from django.contrib.auth.decorators import login_required

def get_db_connection():
    # Helper to connect to the same DuckDB as Crosscheck app
    con = duckdb.connect(os.path.join(settings.BASE_DIR, 'datawarehouse.duckdb'))
    # Ensure sessions table exists (in case Dashboard is hit before any Crosscheck)
    con.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            ovatr VARCHAR PRIMARY KEY,
            company_name VARCHAR,
            tin VARCHAR,
            status VARCHAR,
            total_rows INTEGER,
            match_rate DOUBLE,
            created_at TIMESTAMP,
            last_modified TIMESTAMP
        )
    """)
    return con

@login_required
def index(request):
    """
    Main Dashboard View.
    Displays:
    1. Key Metrics (Total Sessions, Avg Match Rate, Total Rows Processed)
    2. Recent Activity (Last 5 Sessions)
    """
    con = get_db_connection()
    
    try:
        # 1. KPI: Total Sessions
        total_sessions = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        
        # 2. KPI: Average Match Rate (only for completed/processed items)
        avg_match_result = con.execute("SELECT AVG(match_rate) FROM sessions WHERE total_rows > 0").fetchone()
        avg_match_rate = round(avg_match_result[0] or 0.0, 1)

        # 3. KPI: Total Rows Processed
        total_rows = con.execute("SELECT SUM(total_rows) FROM sessions").fetchone()[0] or 0
        
        # 4. Recent Activity (Top 5)
        # We join with company_info just in case, but sessions table has most data now
        recent_sessions = con.execute("""
            SELECT ovatr, company_name, tin, status, match_rate, last_modified
            FROM sessions
            ORDER BY last_modified DESC
            LIMIT 5
        """).fetchall()
        
        # Format for template
        recent_activity = []
        for r in recent_sessions:
            last_mod = r[5]
            time_str = last_mod.strftime('%Y-%m-%d %H:%M') if last_mod else "N/A"
            
            # Calculate time ago roughly for display
            time_ago = "Just now"
            if last_mod:
                diff = datetime.now() - last_mod
                if diff.days > 0: time_ago = f"{diff.days} days ago"
                elif diff.seconds > 3600: time_ago = f"{diff.seconds // 3600} hours ago"
                elif diff.seconds > 60: time_ago = f"{diff.seconds // 60} mins ago"

            recent_activity.append({
                'ovatr': r[0],
                'company_name': r[1],
                'tin': r[2] or 'N/A',
                'status': r[3],
                'match_rate': round(r[4], 1),
                'last_modified': time_str,
                'time_ago': time_ago
            })

    except Exception as e:
        # Fallback if DB not ready
        print(f"Dashboard Error: {e}")
        total_sessions = 0
        avg_match_rate = 0.0
        total_rows = 0
        recent_activity = []
    finally:
        con.close()

    context = {
        'total_sessions': total_sessions,
        'avg_match_rate': avg_match_rate,
        'total_rows': total_rows,
        'recent_activity': recent_activity,
        'user': request.user
    }
    
    return render(request, 'dashboard/index.html', context)