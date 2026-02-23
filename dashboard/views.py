import duckdb
import os
from datetime import datetime
from django.conf import settings
from django.shortcuts import render
from django.contrib.auth.decorators import login_required

def get_db_connection():
    # Helper to connect to the same DuckDB as Crosscheck app
    db_path = os.path.join(settings.BASE_DIR, 'datawarehouse.duckdb')
    con = duckdb.connect(db_path)
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
        # Safely check existing tables to prevent crash on first load
        tables_query = con.execute("SHOW TABLES").fetchall()
        tables = [r[0] for r in tables_query]
        
        total_sessions = 0
        avg_match_rate = 0.0
        
        if 'sessions' in tables:
            total_sessions = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            if total_sessions > 0:
                avg_res = con.execute("SELECT AVG(match_rate) FROM sessions WHERE total_rows > 0").fetchone()
                avg_match_rate = round(avg_res[0] or 0.0, 1)

        # Dynamically calculate TOTAL ROWS from actual master tables
        total_rows = 0
        master_tables = ['purchase', 'sale', 'tax_declaration', 'reverse_charge', 'tax_paid']
        for table in master_tables:
            if table in tables:
                total_rows += con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        # Recent Activity (Top 5)
        recent_activity = []
        if 'sessions' in tables:
            recent_sessions = con.execute("""
                SELECT ovatr, company_name, tin, status, match_rate, last_modified
                FROM sessions
                ORDER BY last_modified DESC
                LIMIT 5
            """).fetchall()
            
            # Format for template
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
                    'match_rate': round(r[4] or 0.0, 1),
                    'last_modified': time_str,
                    'time_ago': time_ago
                })

    except Exception as e:
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