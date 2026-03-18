import duckdb
import os
import pandas as pd
import logging
from datetime import datetime
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required

logger = logging.getLogger(__name__)

def get_db_connection():
    # Helper to connect to the same DuckDB as Crosscheck app
    appdata_dir = os.path.join(os.environ.get('APPDATA'), 'AuditCore PRO')
    db_path = os.path.join(appdata_dir, 'datawarehouse.duckdb')
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

def update_buyer_names(request):
    """
    Handles uploaded Excel file to update English buyer names in the local DuckDB database.
    """
    if request.method == 'POST' and request.FILES.get('company_info_file'):
        excel_file = request.FILES['company_info_file']
        con = None
        
        try:
            # 1. Read the Excel File
            # Removed skiprows=1 because the headers are on the very first row.
            try:
                df = pd.read_excel(excel_file, sheet_name='UPDATE_COMPANY_INFO')
            except ValueError as e:
                messages.error(request, f"Excel parsing error: Make sure the sheet 'UPDATE_COMPANY_INFO' exists. Detail: {str(e)}")
                return redirect('dashboard:index')

            # 2. Prevent KeyErrors: Clean column headers (strip spaces, convert to uppercase)
            df.columns = df.columns.astype(str).str.strip().str.upper()

            # 3. Validate Required Columns before proceeding
            required_cols = ['TAX_REGISTRATION_ID', 'BUYER_NAME']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                error_msg = f"Missing required columns in uploaded file: {', '.join(missing_cols)}. Found: {', '.join(df.columns)}"
                logger.error(error_msg)
                messages.error(request, error_msg)
                return redirect('dashboard:index')

            # 4. Clean the Data
            # Drop empty rows for the specific columns
            df = df.dropna(subset=['TAX_REGISTRATION_ID', 'BUYER_NAME'])
            
            # Extract exactly 9 digits from the Excel TIN (ignores branch codes/hyphens)
            df['CLEAN_TIN'] = df['TAX_REGISTRATION_ID'].astype(str).str.replace(r'\D', '', regex=True).str[:9]
            
            # 5. Connect to DuckDB
            # Ensure the database path matches your project structure (e.g., 'datawarehouse.duckdb')
            con = duckdb.connect('datawarehouse.duckdb')
            
            # Register pandas dataframe as a virtual DuckDB table in memory
            con.register('df_updates', df)
            
            # 6. Execute the Bulk Update
            # Replace 'tax_declaration' with your actual target table (e.g., 'sale')
            update_query = """
                UPDATE tax_declaration 
                SET buyer_name = df_updates.BUYER_NAME
                FROM df_updates
                WHERE LEFT(REGEXP_REPLACE(CAST(tax_declaration.tax_registration_id AS VARCHAR), '[^0-9]', '', 'g'), 9) = df_updates.CLEAN_TIN
            """
            con.execute(update_query)
            
            messages.success(request, f"Buyer names successfully updated to English from the uploaded file!")
            
        except duckdb.Error as db_err:
            error_msg = f"Database update failed. Check if 'tax_declaration' table exists. Error: {str(db_err)}"
            logger.error(error_msg)
            messages.error(request, error_msg)
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            messages.error(request, f"Error processing file: {str(e)}")
            
        finally:
            # 7. Cleanup: ALWAYS run this, even if the code crashes above
            if con:
                try:
                    con.unregister('df_updates')
                except Exception:
                    pass
                con.close()
                
    return redirect('dashboard:index')