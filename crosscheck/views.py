import glob
import os
import io
import json
import re
import time
import duckdb
import warnings
import openpyxl
import ast
import pandas as pd
from copy import copy
from datetime import datetime
from django.conf import settings
from django.shortcuts import render
from django.http import FileResponse, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.storage import FileSystemStorage
from openpyxl import load_workbook
from openpyxl.styles import Font

# --- Helpers ---

def get_db_connection():
    con = duckdb.connect(os.path.join(settings.BASE_DIR, 'datawarehouse.duckdb'))
    
    # Ensure Sessions Table Exists
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

def update_session_metadata(con, ovatr, company_name=None, tin=None, status=None, total_rows=None, match_rate=None):
    """ Helper to upsert session metadata into DuckDB """
    if not ovatr: return
    now = datetime.now()
    
    exists = con.execute("SELECT 1 FROM sessions WHERE ovatr = ?", [ovatr]).fetchone()
    
    if not exists:
        con.execute("""
            INSERT INTO sessions (ovatr, company_name, tin, status, total_rows, match_rate, created_at, last_modified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [ovatr, company_name or 'Unknown', tin or '', status or 'Processing', 0, 0.0, now, now])
    else:
        updates = ["last_modified = ?"]
        params = [now]
        if company_name: 
            updates.append("company_name = ?"); params.append(company_name)
        if tin:
            updates.append("tin = ?"); params.append(tin)
        if status:
            updates.append("status = ?"); params.append(status)
        if total_rows is not None:
            updates.append("total_rows = ?"); params.append(total_rows)
        if match_rate is not None:
            updates.append("match_rate = ?"); params.append(match_rate)
            
        params.append(ovatr)
        query = f"UPDATE sessions SET {', '.join(updates)} WHERE ovatr = ?"
        con.execute(query, params)

def clean_currency(val):
    s = str(val).strip()
    if s.lower() in ['nan', 'none', '', 'nat', '-']:
        return 0.0
    # Remove non-numeric chars except . and -
    clean_s = re.sub(r'[^\d.-]', '', s)
    # Handle accounting format (100) -> -100
    if '(' in s and ')' in s:
        clean_s = '-' + re.sub(r'[^\d.]', '', s)
    try:
        return float(clean_s)
    except ValueError:
        return 0.0

def clean_invoice_text(val):    
    """ 
    Removes all special characters (dashes, spaces, dots, etc.).
    Keeps only Alphanumeric characters (A-Z, 0-9).
    """
    if not val:
        return ""
    # Regex: Replace anything that is NOT a-z, A-Z, or 0-9 with empty string
    return re.sub(r'[^a-zA-Z0-9]', '', str(val))

def cleanup_old_files():
    """ Deletes files in temp_uploads/reports older than 24 hours. """
    directories = [
        os.path.join(settings.MEDIA_ROOT, 'temp_uploads'),
        os.path.join(settings.MEDIA_ROOT, 'temp_reports')
    ]
    current_time = time.time()
    for folder in directories:
        if not os.path.exists(folder): continue
        for f in glob.glob(os.path.join(folder, '*')):
            try:
                if current_time - os.path.getctime(f) > 86400: os.remove(f)
            except: pass

# --- Views ---

def new_crosscheck(request):
    """ Renders the 'New Crosscheck' upload page. """
    return render(request, 'crosscheck/new.html')

def processing_view(request):
    """ 
    Renders the 'Processing' page. 
    Retrieves the OVATR code from URL first, falls back to Session.
    """
    code = request.GET.get('ovatr_code') or request.session.get('ovatr_code', '')
    context = {'ovatr_code': code}
    return render(request, 'crosscheck/processing.html', context)

def results_view(request):
    """ 
    Renders the 'Results' dashboard. 
    Retrieves the OVATR code from URL first, falls back to Session.
    """
    code = request.GET.get('ovatr_code') or request.session.get('ovatr_code', '')
    context = {'ovatr_code': code}
    return render(request, 'crosscheck/results.html', context)

def history_view(request):
    """ Renders the History UI """
    return render(request, 'crosscheck/history.html')

def report_view(request):
    """ Renders the main Reporting Module UI """
    code = request.GET.get('ovatr_code') or request.session.get('ovatr_code', '')
    return render(request, 'crosscheck/report.html', {'ovatr_code': code})

# --- API: History Data ---

def get_history_api(request):
    try:
        query = request.GET.get('q', '').strip()
        conn = get_db_connection()
        
        sql = """
            SELECT s.ovatr, s.company_name, s.status, s.total_rows, s.match_rate, s.last_modified, c."vatin" as tin
            FROM sessions s
            LEFT JOIN company_info c ON s.ovatr = c."ovatr"
        """
        params = []
        where_clauses = []

        if query:
            where_clauses.append("(s.ovatr ILIKE ? OR s.company_name ILIKE ? OR c.\"vatin\" ILIKE ?)")
            params = [f'%{query}%', f'%{query}%', f'%{query}%']
            
        if where_clauses: sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY s.last_modified DESC LIMIT 50"
        
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        
        data = []
        for r in rows:
            last_mod = r[5]
            time_ago = "Just now"
            if last_mod:
                diff = datetime.now() - last_mod
                if diff.days > 0: time_ago = f"{diff.days} days ago"
                elif diff.seconds > 3600: time_ago = f"{diff.seconds // 3600} hours ago"
                elif diff.seconds > 60: time_ago = f"{diff.seconds // 60} mins ago"

            data.append({
                'ovatr': r[0], 'company_name': r[1], 'status': r[2],
                'total_rows': r[3], 'match_rate': round(r[4], 1),
                'last_modified': last_mod.strftime('%Y-%m-%d %H:%M') if last_mod else '',
                'tin': r[6] or 'N/A', 'time_ago': time_ago
            })
        return JsonResponse({'status': 'success', 'data': data})
    except Exception as e: return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

# --- Upload & Save APIs ---

@csrf_exempt
def upload_init(request):
    """ Handles the initial file upload and reads Company Info for preview. """
    cleanup_old_files()
    if request.method == 'POST' and request.FILES.get('file'):
        file = request.FILES['file']
        fs = FileSystemStorage()
        clean_name = fs.get_available_name(file.name)
        filename = fs.save(os.path.join("temp", clean_name), file)
        uploaded_file_path = fs.path(filename)

        try:
            # Try specific sheet name, fallback to first sheet
            try:
                df = pd.read_excel(uploaded_file_path, sheet_name='COMPANY INFO', header=None)
            except:
                df = pd.read_excel(uploaded_file_path, sheet_name=0, header=None)
            
            data_map = {
                'company_name_kh': '', 'company_name_en': '', 'file_barcode': '',
                'old_vatin': '', 'vatin': '', 'enterprise_id': '',
                'registered_entity': '', 'reg_date': '', 'success_date': '', 
                'taxpayer_type': '', 'status': '', 'tax_year': '',
                'address_main': '', 'address_office': '', 'phone': '', 'email': '',
                'employee_count': '', 'total_salary': '', 'property_type': '',
                'rent_per_month': '', 'enterprise_form': '', 'add_ent_form': '', 'signage': '',
                'h_date': '', 'h_real_12m': '', 'h_real_3m': '', 'h_est_12m': '', 'h_est_3m': '',
                'business_activities': [], 'enterprise_accounts': [], 'related_institutions': []
            }

            def get_val_safe(val):
                s = str(val).strip() if pd.notna(val) else ""
                if s.endswith(".0"): s = s[:-2]
                return s

            def get_col(row, idx):
                return get_val_safe(row[idx]) if idx < len(row) else ""

            def extract_val_smart(row):
                c0 = get_col(row, 0)
                val = ""
                if '៖' in c0:
                    parts = c0.split('៖')
                    if len(parts) > 1: val = parts[1].strip()
                if not val: val = get_col(row, 1)
                return val.replace('"', '').replace("'", "")

            def clean_khmer_only(text):
                if pd.isna(text): return ""
                text = str(text)
                cleaned = re.sub(r'[A-Za-z]', '', text)
                return " ".join(cleaned.split())

            current_section = None 
            header_found = False
            estimate_header_index = None

            for index, row in df.iterrows():
                cell_0 = get_col(row, 0)
                
                if "ការប៉ាន់ស្មានផលរបរ" in cell_0:
                    estimate_header_index = index
                
                if "ឈ្មោះសហគ្រាសជាអក្សរខ្មែរ" in cell_0: data_map['company_name_kh'] = extract_val_smart(row)
                elif "ឈ្មោះសហគ្រាសជាអក្សរឡាតាំង" in cell_0: data_map['company_name_en'] = extract_val_smart(row)
                elif "លេខបារកូដឯកសារ" in cell_0: data_map['file_barcode'] = extract_val_smart(row)
                elif "លេខអត្តសញ្ញាណកម្មចាស់" in cell_0: data_map['old_vatin'] = extract_val_smart(row)
                elif "លេខអត្តសញ្ញាណកម្ម" in cell_0: data_map['vatin'] = extract_val_smart(row)
                elif "លេខកាតសម្គាល់សហគ្រាស" in cell_0: data_map['enterprise_id'] = extract_val_smart(row)
                elif "ចុះបញ្ជីនៅ" in cell_0: data_map['registered_entity'] = extract_val_smart(row)
                elif "កាលបរិច្ឆេទចុះបញ្ជី" in cell_0: data_map['reg_date'] = extract_val_smart(row)
                elif "កាលបរិច្ឆេទជោគជ័យ" in cell_0: data_map['success_date'] = extract_val_smart(row)
                elif "ប្រភេទអ្នកជាប់ពន្ធ" in cell_0: data_map['taxpayer_type'] = extract_val_smart(row)
                elif "ស្ថានភាព" in cell_0: data_map['status'] = extract_val_smart(row)
                elif "ទ្រង់ទ្រាយសហគ្រាស" in cell_0 and "បន្ថែម" not in cell_0: data_map['enterprise_form'] = extract_val_smart(row)
                elif "ទ្រង់ទ្រាយសហគ្រាសបន្ថែម" in cell_0: data_map['add_ent_form'] = extract_val_smart(row)
                elif "ឆ្នាំជាប់ពន្ធ" in cell_0: data_map['tax_year'] = extract_val_smart(row)
                elif "អាសយដ្ឋានអាជីវកម្មគោលដេីម" in cell_0: data_map['address_main'] = extract_val_smart(row)
                elif "អាសយដ្ឋានទីចាត់ការ" in cell_0: data_map['address_office'] = extract_val_smart(row)
                elif "លេខទូរសព្ទ" in cell_0: data_map['phone'] = extract_val_smart(row)
                elif "សារអេឡិចត្រូនិក" in cell_0: data_map['email'] = extract_val_smart(row)
                elif "អចលនទ្រព្យ" in cell_0: data_map['property_type'] = extract_val_smart(row)
                elif "ផ្លាកយីហោ" in cell_0: data_map['signage'] = extract_val_smart(row)
                elif "ថ្លៃឈ្នួល/១ខែ" in cell_0: data_map['rent_per_month'] = extract_val_smart(row)
                elif "ចំនួននិយោជិក" in cell_0: data_map['employee_count'] = extract_val_smart(row)
                elif "ប្រាក់ខែសរុប" in cell_0: data_map['total_salary'] = extract_val_smart(row)

                if "សកម្មភាពអាជីវកម្ម" in cell_0:
                    current_section = 'business_activities'; header_found = False; continue
                elif "គណនីសហគ្រាស" in cell_0:
                    current_section = 'enterprise_accounts'; header_found = False; continue
                elif "ស្ថាប័នពាក់ព័ន្ធ" in cell_0:
                    current_section = 'related_institutions'; header_found = False; continue
                
                if current_section:
                    is_empty = all(pd.isna(val) or str(val).strip() == "" for val in row[:3])
                    if is_empty:
                        if header_found: current_section = None
                        continue

                    if not header_found:
                        row_str = str(row.values).lower()
                        if "ល.រ" in row_str or "no" in row_str or "code" in row_str: header_found = True
                        continue

                    if current_section == 'business_activities':
                        data_map['business_activities'].append({
                            'no': get_col(row, 1), 'code': get_col(row, 2),
                            'name': clean_khmer_only(get_col(row, 3)), 'desc': clean_khmer_only(get_col(row, 4)),
                            'type': get_col(row, 5)
                        })
                    elif current_section == 'enterprise_accounts':
                        # Mapping: Col 3 = Number, Col 4 = Account Name, Col 2 = Bank
                        data_map['enterprise_accounts'].append({
                            'no': get_col(row, 1), 
                            'bank': get_col(row, 2), 
                            'number': get_col(row, 3), 
                            'account_name': get_col(row, 4), 
                            'currency': get_col(row, 5), 
                            'type': get_col(row, 6)
                        })
                    elif current_section == 'related_institutions':
                        data_map['related_institutions'].append({
                            'no': get_col(row, 1), 'name': get_col(row, 2), 
                            'ref': get_col(row, 3), 'date': get_col(row, 4)
                        })

            if estimate_header_index is not None:
                i = estimate_header_index
                def get_cell(r, c):
                    if r < len(df) and c < len(df.columns): return get_val_safe(df.iat[r, c])
                    return ""
                data_map['h_date'] = get_cell(i + 2, 2)
                data_map['h_real_12m'] = get_cell(i + 4, 2)
                data_map['h_real_3m'] = get_cell(i + 4, 3)
                data_map['h_est_12m'] = get_cell(i + 5, 2)
                data_map['h_est_3m'] = get_cell(i + 5, 3)

            return JsonResponse({'status': 'success', 'data': data_map, 'temp_path': filename})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})
            
    return JsonResponse({'status': 'error', 'message': 'No file provided'})

@csrf_exempt
def save_company_info(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            clean_data = {k.lower(): v for k, v in data.items()}
            
            if 'ovatr' not in clean_data or not clean_data['ovatr']:
                return JsonResponse({'status': 'error', 'message': 'Missing Critical Field: OVATR'}, status=400)

            ovatr = clean_data['ovatr']
            comp_name = clean_data.get('company_name_kh') or clean_data.get('company_name_en') or 'Unknown Company'

            request.session['ovatr_code'] = ovatr

            con = get_db_connection()
            
            # 1. Save to Company Info Table
            columns_schema = []
            for key in clean_data.keys():
                if key == 'ovatr': columns_schema.append(f'"{key}" VARCHAR PRIMARY KEY')
                else: columns_schema.append(f'"{key}" VARCHAR')
            con.execute(f"CREATE TABLE IF NOT EXISTS company_info ({', '.join(columns_schema)})")
            columns = [f'"{k}"' for k in clean_data.keys()]
            placeholders = ['?'] * len(clean_data)
            values = list(clean_data.values())
            con.execute(f"INSERT OR REPLACE INTO company_info ({', '.join(columns)}) VALUES ({', '.join(placeholders)})", values)
            
            # 2. Update Session History (No need to pass TIN manually anymore, fetching via JOIN)
            update_session_metadata(con, ovatr, company_name=comp_name, status="Processing")

            con.close()
            return JsonResponse({'status': 'success', 'message': 'Company Info saved successfully'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid method'}, status=405)

@csrf_exempt
def save_taxpaid(request):
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            ovatr_val = body.get('ovatr') or body.get('OVATR')

            fs = FileSystemStorage()
            full_path = fs.path(body['temp_path'])
            try:
                df = pd.read_excel(full_path, sheet_name='TAXPAID', header=None)
            except ValueError:
                return JsonResponse({'status': 'error', 'message': 'Sheet "TAXPAID" not found'}, status=400)

            extracted_rows = []
            current_year = None
            
            def clean_money(val):
                s = str(val).strip().replace(',', '')
                if s in ['-', '', 'nan', 'None']: return 0.0
                try: return float(s)
                except ValueError: return 0.0

            for idx, row in df.iterrows():
                row_vals = [str(x).strip() for x in row.values]
                col0 = row_vals[0] if len(row_vals) > 0 else ""
                col1 = row_vals[1] if len(row_vals) > 1 else ""

                if "ព័ត៌មានលម្អិតប្រចាំឆ្នាំ" in col0:
                    found_year = None
                    if col1.isdigit(): found_year = col1
                    elif re.search(r'\d{4}', col0): found_year = re.search(r'\d{4}', col0).group()
                    if found_year: current_year = found_year
                    continue

                if "មករា" in str(row_vals): continue

                if current_year and len(row_vals) > 15:
                    description = row_vals[2]
                    if not description or description.lower() in ['nan', 'close', ''] or description == "ឆ្នាំបង់ពន្ធ": continue

                    extracted_rows.append({
                        'ovatr': ovatr_val, 'tax_year': current_year, 'description': description,
                        'jan': clean_money(row.values[3]), 'feb': clean_money(row.values[4]), 'mar': clean_money(row.values[5]),
                        'apr': clean_money(row.values[6]), 'may': clean_money(row.values[7]), 'jun': clean_money(row.values[8]),
                        'jul': clean_money(row.values[9]), 'aug': clean_money(row.values[10]), 'sep': clean_money(row.values[11]),
                        'oct': clean_money(row.values[12]), 'nov': clean_money(row.values[13]), 'dec': clean_money(row.values[14]),
                        'total': clean_money(row.values[15]),
                    })

            if extracted_rows:
                con = get_db_connection()
                con.execute("CREATE TABLE IF NOT EXISTS tax_paid (ovatr VARCHAR, tax_year VARCHAR, description VARCHAR, jan DOUBLE, feb DOUBLE, mar DOUBLE, apr DOUBLE, may DOUBLE, jun DOUBLE, jul DOUBLE, aug DOUBLE, sep DOUBLE, oct DOUBLE, nov DOUBLE, dec DOUBLE, total DOUBLE, PRIMARY KEY (ovatr, tax_year, description))")
                con.execute("DELETE FROM tax_paid WHERE ovatr = ?", [ovatr_val])
                con.executemany("INSERT INTO tax_paid VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [list(d.values()) for d in extracted_rows])
                con.close()
                return JsonResponse({'status': 'success', 'message': f'Saved {len(extracted_rows)} records for TaxPaid.'})
            return JsonResponse({'status': 'warning', 'message': 'No valid tax data found in TAXPAID sheet.'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid Method'}, status=405)

@csrf_exempt
def save_purchase(request):
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            ovatr_val = body.get('ovatr') or body.get('OVATR')

            fs = FileSystemStorage()
            try:
                df = pd.read_excel(fs.path(body['temp_path']), sheet_name='PURCHASE', header=None)
            except ValueError:
                return JsonResponse({'status': 'error', 'message': 'Sheet "PURCHASE" not found'}, status=400)

            df = df.iloc[3:] # Skip 3 rows
            if len(df.columns) < 17:
                return JsonResponse({'status': 'error', 'message': f'Format Mismatch: Expected 17 columns (A-Q), found {len(df.columns)}.'})

            # Lowercase target columns
            target_cols = [
                'excel_no', 'date', 'invoice_no', 'type', 'supplier_tin', 'supplier_name', 
                'total_amount', 'exclude_vat', 'non_vat_purchase', 'vat_0', 
                'purchase', 'import', 'non_creditable_vat', 'state_charge', 'non_state_charge', 
                'description', 'status'
            ]
            df = df.iloc[:, :17]; df.columns = target_cols
            df = df[df['date'].notna()]
            df['no'] = range(1, len(df) + 1); df['no'] = df['no'].astype(str)

            for col in ['total_amount', 'exclude_vat', 'non_vat_purchase', 'vat_0', 'purchase', 'import', 'non_creditable_vat', 'state_charge', 'non_state_charge']:
                df[col] = df[col].apply(clean_currency)

            df['ovatr'] = ovatr_val
            
            con = get_db_connection()
            con.execute("""
                CREATE TABLE IF NOT EXISTS purchase (
                    ovatr VARCHAR, no VARCHAR, date VARCHAR, invoice_no VARCHAR, type VARCHAR, 
                    supplier_tin VARCHAR, supplier_name VARCHAR, total_amount DOUBLE, 
                    exclude_vat DOUBLE, non_vat_purchase DOUBLE, vat_0 DOUBLE, purchase DOUBLE, 
                    import DOUBLE, non_creditable_vat DOUBLE, state_charge DOUBLE, 
                    non_state_charge DOUBLE, description VARCHAR, status VARCHAR, 
                    PRIMARY KEY (ovatr, no)
                )
            """)
            con.execute("DELETE FROM purchase WHERE ovatr = ?", [ovatr_val])
            con.register('df_purchase', df)
            con.execute("""
                INSERT INTO purchase 
                SELECT 
                    ovatr, no, date, invoice_no, type, supplier_tin, supplier_name, 
                    total_amount, exclude_vat, non_vat_purchase, vat_0, purchase, 
                    import, non_creditable_vat, state_charge, non_state_charge, 
                    description, status 
                FROM df_purchase
            """)
            con.close()
            return JsonResponse({'status': 'success', 'message': f'Saved {len(df)} Purchase Invoices.'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid Method'}, status=405)

@csrf_exempt
def save_sale(request):
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            ovatr_val = body.get('ovatr') or body.get('OVATR')

            fs = FileSystemStorage()
            try:
                df = pd.read_excel(fs.path(body['temp_path']), sheet_name='SALE', header=None)
            except ValueError:
                return JsonResponse({'status': 'error', 'message': 'Sheet "SALE" not found'}, status=400)

            df = df.iloc[3:] # Skip 3 rows
            if len(df.columns) < 23:
                 return JsonResponse({'status': 'error', 'message': f'Format Mismatch: Expected 23+ columns (A-W), found {len(df.columns)}'})

            target_cols = [
                'excel_no', 'date', 'invoice_no', 'credit_note_no', 'buyer_type', 'tax_registration_id', 
                'buyer_name', 'total_invoice_amount', 'amount_exclude_vat', 'non_vat_sales', 
                'vat_zero_rate', 'vat_local_sale', 'vat_export', 'vat_local_sale_state_burden', 
                'vat_withheld_by_national_treasury', 'plt', 'special_tax_on_goods', 
                'special_tax_on_services', 'accommodation_tax', 'income_tax_redemption_rate', 
                'notes', 'description', 'tax_declaration_status'
            ]
            df = df.iloc[:, :23]; df.columns = target_cols
            df = df[df['date'].notna()]
            df['no'] = range(1, len(df) + 1); df['no'] = df['no'].astype(str)

            numeric_cols = [
                'total_invoice_amount', 'amount_exclude_vat', 'non_vat_sales', 'vat_zero_rate', 
                'vat_local_sale', 'vat_export', 'vat_local_sale_state_burden', 
                'vat_withheld_by_national_treasury', 'plt', 'special_tax_on_goods', 
                'special_tax_on_services', 'accommodation_tax', 'income_tax_redemption_rate'
            ]
            for col in numeric_cols:
                df[col] = df[col].apply(clean_currency)

            df['ovatr'] = ovatr_val
            
            con = get_db_connection()
            con.execute("""
                CREATE TABLE IF NOT EXISTS sale (
                    ovatr VARCHAR, no VARCHAR, date VARCHAR, invoice_no VARCHAR, credit_note_no VARCHAR,
                    buyer_type VARCHAR, tax_registration_id VARCHAR, buyer_name VARCHAR,
                    total_invoice_amount DOUBLE, amount_exclude_vat DOUBLE, non_vat_sales DOUBLE,
                    vat_zero_rate DOUBLE, vat_local_sale DOUBLE, vat_export DOUBLE,
                    vat_local_sale_state_burden DOUBLE, vat_withheld_by_national_treasury DOUBLE, plt DOUBLE,
                    special_tax_on_goods DOUBLE, special_tax_on_services DOUBLE, accommodation_tax DOUBLE,
                    income_tax_redemption_rate DOUBLE, notes VARCHAR, description VARCHAR,
                    tax_declaration_status VARCHAR, PRIMARY KEY (ovatr, no)
                )
            """)
            con.execute("DELETE FROM sale WHERE ovatr = ?", [ovatr_val])
            con.register('df_sale', df)
            con.execute("""
                INSERT INTO sale 
                SELECT 
                    ovatr, no, date, invoice_no, credit_note_no, buyer_type, 
                    tax_registration_id, buyer_name, total_invoice_amount, 
                    amount_exclude_vat, non_vat_sales, vat_zero_rate, 
                    vat_local_sale, vat_export, vat_local_sale_state_burden, 
                    vat_withheld_by_national_treasury, plt, special_tax_on_goods, 
                    special_tax_on_services, accommodation_tax, 
                    income_tax_redemption_rate, notes, description, 
                    tax_declaration_status
                FROM df_sale
            """)
            con.close()
            return JsonResponse({'status': 'success', 'message': f'Saved {len(df)} Sale Invoices.'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid Method'}, status=405)

@csrf_exempt
def save_reverse_charge(request):
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            ovatr_val = body.get('ovatr') or body.get('OVATR')

            fs = FileSystemStorage()
            try:
                try: df = pd.read_excel(fs.path(body['temp_path']), sheet_name='REVERSE_CHARGE', header=None)
                except: df = pd.read_excel(fs.path(body['temp_path']), sheet_name='REVERSE CHARGE', header=None)
            except ValueError:
                return JsonResponse({'status': 'error', 'message': 'Sheet "REVERSE_CHARGE" not found'}, status=400)

            df = df.iloc[3:]
            if len(df.columns) < 14:
                 return JsonResponse({'status': 'error', 'message': f'Format Mismatch: Expected 14+ columns, found {len(df.columns)}'})

            target_cols = [
                'excel_no', 'date', 'invoice_no', 'supplier_non_resident', 'supplier_tin', 
                'supplier_name', 'address', 'email', 'non_vat_supply', 'exclude_vat', 
                'vat', 'description', 'status', 'declaration_status'
            ]
            df = df.iloc[:, :14]; df.columns = target_cols
            df = df[df['date'].notna()]
            df['no'] = range(1, len(df) + 1); df['no'] = df['no'].astype(str)

            for col in ['non_vat_supply', 'exclude_vat', 'vat']:
                df[col] = df[col].apply(clean_currency)

            df['ovatr'] = ovatr_val
            
            con = get_db_connection()
            con.execute("""
                CREATE TABLE IF NOT EXISTS reverse_charge (
                    ovatr VARCHAR, no VARCHAR, date VARCHAR, invoice_no VARCHAR, 
                    supplier_non_resident VARCHAR, supplier_tin VARCHAR, supplier_name VARCHAR, 
                    address VARCHAR, email VARCHAR, non_vat_supply DOUBLE, exclude_vat DOUBLE, 
                    vat DOUBLE, description VARCHAR, status VARCHAR, declaration_status VARCHAR, 
                    PRIMARY KEY (ovatr, no)
                )
            """)
            con.execute("DELETE FROM reverse_charge WHERE ovatr = ?", [ovatr_val])
            con.register('df_rc', df)
            con.execute("""
                INSERT INTO reverse_charge 
                SELECT 
                    ovatr, no, date, invoice_no, supplier_non_resident, 
                    supplier_tin, supplier_name, address, email, 
                    non_vat_supply, exclude_vat, vat, description, 
                    status, declaration_status 
                FROM df_rc
            """)
            con.close()
            return JsonResponse({'status': 'success', 'message': f'Saved {len(df)} Reverse Charge Records.'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid Method'}, status=405)

# --- Analytics & Reporting ---

@csrf_exempt
def update_result_row(request):
    """
    Updates a specific row in the DuckDB 'purchase' table based on edits in the Results UI.
    Receives: { ovatr: str, type: 'local'|'import', no: str, updates: { field: value } }
    
    UPDATED: Now also handles 'tax_declaration' updates if d_* fields are present.
    """
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            ovatr = body.get('ovatr')
            row_no = body.get('no') 
            updates = body.get('updates', {})
            # Extract history object sent from frontend
            history_data = body.get('history', {}) 
            
            # 1. Update Purchase Table
            db_updates = {}
            if 'p_desc' in updates: db_updates['description'] = updates['p_desc']
            if 'p_supp' in updates: db_updates['supplier_name'] = updates['p_supp']
            if 'p_tin' in updates: db_updates['supplier_tin'] = updates['p_tin']
            if 'p_inv' in updates: db_updates['invoice_no'] = updates['p_inv']
            if 'p_date' in updates: db_updates['date'] = updates['p_date']
            
            if 'p_amt' in updates:
                amt = clean_currency(updates['p_amt'])
                table_type = body.get('type', 'local')
                if table_type == 'import':
                    db_updates['"import"'] = amt
                else:
                    db_updates['purchase'] = amt

            con = get_db_connection()
            
            # --- HISTORY LOGGING ---
            # Create Table if not exists
            con.execute("""
                CREATE TABLE IF NOT EXISTS change_history (
                    timestamp TIMESTAMP,
                    ovatr VARCHAR,
                    row_no VARCHAR,
                    table_type VARCHAR,
                    field VARCHAR,
                    old_value VARCHAR,
                    new_value VARCHAR
                )
            """)

            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            table_type = body.get('type', 'local')
            
            # Log changes
            for field, vals in history_data.items():
                old_v = str(vals.get('old', ''))
                new_v = str(vals.get('new', ''))
                # Only log if actually different (frontend should handle this but safety check)
                if old_v != new_v:
                    con.execute("INSERT INTO change_history VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                [current_time, ovatr, str(row_no), table_type, field, old_v, new_v])

            # --- PERFORM UPDATE ---
            if db_updates:
                set_clause = ", ".join([f"{k} = ?" for k in db_updates.keys()])
                params = list(db_updates.values())
                params.extend([ovatr, str(row_no)]) 
                query = f"UPDATE purchase SET {set_clause} WHERE ovatr = ? AND no = ?"
                con.execute(query, params)

            # 2. Update Tax Declaration Table (if fields provided)
            d_updates = {}
            if 'd_inv' in updates: d_updates['invoice_number'] = updates['d_inv']
            if 'd_tin' in updates: d_updates['tax_registration_id'] = updates['d_tin']
            if 'd_name' in updates: d_updates['buyer_name'] = updates['d_name']
            if 'd_date' in updates: d_updates['date'] = updates['d_date']
            
            if 'original_d_inv' in updates and 'original_d_tin' in updates and d_updates:
                d_set_clause = [f"{k} = ?" for k in d_updates.keys()]
                d_params = list(d_updates.values())
                d_params.append(updates['original_d_inv'])
                d_params.append(updates['original_d_tin'])
                q_dec = f"UPDATE tax_declaration SET {', '.join(d_set_clause)} WHERE invoice_number = ? AND tax_registration_id = ?"
                con.execute(q_dec, d_params)

            # --- UPDATE SESSION TIMESTAMP ---
            update_session_metadata(con, ovatr)

            con.close()
            
            return JsonResponse({'status': 'success', 'message': 'Row updated'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid Method'}, status=405)

def get_row_history(request):
    """
    Fetches the history for a specific row.
    """
    try:
        ovatr = request.GET.get('ovatr')
        row_no = request.GET.get('no')
        if not ovatr or not row_no:
            return JsonResponse({'status': 'error', 'message': 'Missing params'}, status=400)

        con = get_db_connection()
        # Check if table exists
        try:
            con.execute("SELECT 1 FROM change_history LIMIT 1")
        except:
            con.close()
            return JsonResponse({'status': 'success', 'data': []}) # No history yet

        data = con.execute("""
            SELECT timestamp, field, old_value, new_value 
            FROM change_history 
            WHERE ovatr = ? AND row_no = ? 
            ORDER BY timestamp DESC
        """, [ovatr, row_no]).fetchall()
        con.close()

        history = []
        for row in data:
            history.append({
                'timestamp': str(row[0]),
                'field': row[1],
                'old_value': row[2],
                'new_value': row[3]
            })
            
        return JsonResponse({'status': 'success', 'data': history})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def get_crosscheck_stats(request):
    """
    UPDATED: Matches d.tax_registration_id with company_info.vatin
    instead of purchase.supplier_tin.
    """
    ovatr_code = request.GET.get('ovatr_code') or request.session.get('ovatr_code')
    if not ovatr_code: return JsonResponse({'status': 'error'}, status=400)

    try:
        conn = get_db_connection()
        
        # 1. Counts
        res_p = conn.execute("""
            SELECT 
                COUNT(CASE WHEN purchase > 0 THEN 1 END),
                COUNT(CASE WHEN "import" > 0 THEN 1 END)
            FROM purchase WHERE ovatr = ?
        """, [ovatr_code]).fetchone()
        
        count_local = res_p[0] if res_p else 0
        count_import = res_p[1] if res_p else 0
        total_rows = count_local + count_import
        
        # 2. Strict Matches (Declarations)
        # JOIN company_info to get the User's VATIN
        res_d = conn.execute("""
            SELECT COUNT(DISTINCT d.id)
            FROM tax_declaration d
            JOIN purchase p ON 
                regexp_replace(upper(d.invoice_number), '[^A-Z0-9]', '', 'g') = regexp_replace(upper(p.invoice_no), '[^A-Z0-9]', '', 'g')
            JOIN company_info c ON p.ovatr = c.ovatr
            WHERE p.ovatr = ?
            AND regexp_replace(upper(d.tax_registration_id), '[^A-Z0-9]', '', 'g') = regexp_replace(upper(c.vatin), '[^A-Z0-9]', '', 'g')
            AND month(d.date) = month(COALESCE(try_cast(p.date as DATE), strptime(p.date, '%d-%m-%Y')))
            AND year(d.date) = year(COALESCE(try_cast(p.date as DATE), strptime(p.date, '%d-%m-%Y')))
        """, [ovatr_code]).fetchone()
        
        count_d = res_d[0] if res_d else 0
        
        match_rate = (count_d / total_rows * 100) if total_rows > 0 else 0.0
        update_session_metadata(conn, ovatr_code, total_rows=total_rows, match_rate=match_rate, status="Completed")
        conn.close()
        
        file_path = os.path.join(settings.MEDIA_ROOT, 'temp_reports', f"AnnexIII_{ovatr_code}.xlsx")
        
        return JsonResponse({
            'status': 'success',
            'total_rows': max(total_rows, count_d),
            'purchase_count': total_rows, 
            'local_count': count_local,
            'import_count': count_import,
            'declaration_count': count_d,
            'is_ready': os.path.exists(file_path)
        })
    except Exception as e: return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def generate_annex_iii(request):
    """
    UPDATED:
    1. Duplicates 'AnnexIII-Import' from 'AnnexIII-Local Pur' BEFORE populating the Local Pur sheet.
    2. Dynamically copies formatting (fonts, borders, alignment) from the start_row to all new rows.
    3. CLEANUP STEP: Deletes existing data rows before writing new data to remove dummy/template data.
    4. FIXED: "No" column now starts at 1 (using i+1 instead of r-1).
    """
    conn = None
    try:
        template_path = os.path.join(settings.BASE_DIR, 'core', 'templates', 'static', 'CC - guide.xlsx')
        if not os.path.exists(template_path): template_path = os.path.join(settings.BASE_DIR, 'core', 'static', 'CC - guide.xlsx')
        
        ovatr_code = request.GET.get('ovatr_code') or request.session.get('ovatr_code')
        if not ovatr_code: return JsonResponse({'status': 'error', 'message': 'Session ID missing'}, status=400)

        conn = get_db_connection()

        # FETCH USER VATIN
        vatin_row = conn.execute("SELECT vatin FROM company_info WHERE ovatr = ?", [ovatr_code]).fetchone()
        user_vatin = vatin_row[0] if vatin_row else ""
        user_vatin_safe = user_vatin.replace('"', '""') 

        # Fetch Purchases
        local_purchases = conn.execute("""
            SELECT description, supplier_name, supplier_tin, invoice_no, date, purchase, no 
            FROM purchase WHERE ovatr = ? AND purchase > 0 ORDER BY CAST(no AS INTEGER) ASC
        """, [ovatr_code]).fetchall()

        import_purchases = conn.execute("""
            SELECT description, supplier_name, supplier_tin, invoice_no, date, "import", no
            FROM purchase WHERE ovatr = ? AND "import" > 0 ORDER BY CAST(no AS INTEGER) ASC
        """, [ovatr_code]).fetchall()

        # Fetch Matched Declarations
        raw_decs = conn.execute("""
            SELECT d.date, d.invoice_number, d.tax_registration_id, d.buyer_name, d.vat_local_sale, d.vat_export, p.invoice_no
            FROM tax_declaration d
            JOIN purchase p ON 
                regexp_replace(upper(d.invoice_number), '[^A-Z0-9]', '', 'g') = regexp_replace(upper(p.invoice_no), '[^A-Z0-9]', '', 'g')
            JOIN company_info c ON p.ovatr = c.ovatr
            WHERE p.ovatr = ?
            AND regexp_replace(upper(d.tax_registration_id), '[^A-Z0-9]', '', 'g') = regexp_replace(upper(c.vatin), '[^A-Z0-9]', '', 'g')
            AND month(d.date) = month(COALESCE(try_cast(p.date as DATE), strptime(p.date, '%d-%m-%Y')))
            AND year(d.date) = year(COALESCE(try_cast(p.date as DATE), strptime(p.date, '%d-%m-%Y')))
        """, [ovatr_code]).fetchall()
        
        dec_map = {}
        for dec in raw_decs:
            p_inv_key = clean_invoice_text(dec[6])
            if p_inv_key: dec_map[p_inv_key] = dec

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            wb = load_workbook(template_path)
        
        # Create Import sheet BEFORE populating Local sheet to ensure clean template copy
        if 'AnnexIII-Import' not in wb.sheetnames and 'AnnexIII-Local Pur' in wb.sheetnames:
            target = wb.copy_worksheet(wb['AnnexIII-Local Pur'])
            target.title = 'AnnexIII-Import'

        def process_sheet(sheet_name, data_rows):
            if sheet_name not in wb.sheetnames: return
            ws = wb[sheet_name]
            
            # 1. Identify start row
            start_row = 8
            for r in range(1, 15):
                if ws.cell(row=r, column=1).value and "ល.រ" in str(ws.cell(row=r, column=1).value):
                    start_row = r + 1; break
            
            # 2. Capture Styles from the first data row (start_row)
            style_source = {}
            for c in range(1, 25): # Columns A to X
                source_cell = ws.cell(row=start_row, column=c)
                style_source[c] = {
                    'font': copy(source_cell.font),
                    'border': copy(source_cell.border),
                    'fill': copy(source_cell.fill),
                    'number_format': source_cell.number_format,
                    'alignment': copy(source_cell.alignment),
                    'protection': copy(source_cell.protection)
                }

            # 3. CLEANUP: Delete existing data to ensure sheet is empty of template garbage
            if ws.max_row >= start_row:
                 ws.delete_rows(start_row, ws.max_row - start_row + 1)
            
            # 4. Write Data & Apply Styles
            for i, p_row in enumerate(data_rows):
                r = start_row + i
                p_inv_val = p_row[3] or ""
                p_inv_clean = clean_invoice_text(p_inv_val)

                # Write values (No Column adjusted to start at 1)
                ws.cell(row=r, column=1, value=i+1) 
                ws.cell(row=r, column=2, value=p_row[0] or "")
                ws.cell(row=r, column=3, value=p_row[1] or "")
                ws.cell(row=r, column=4, value=p_row[2] or "")
                ws.cell(row=r, column=5, value=p_inv_val)
                ws.cell(row=r, column=6, value=p_row[4] or "")
                ws.cell(row=r, column=7, value=p_row[5] if p_row[5] else 0)
                
                ws.cell(row=r, column=9, value=f"=G{r}")
                ws.cell(row=r, column=11, value=f"=I{r}-G{r}")

                d_row = dec_map.get(p_inv_clean)
                d_inv_val = ""
                if d_row:
                    d_inv_val = d_row[1]
                    ws.cell(row=r, column=19, value=d_row[0] or "")
                    ws.cell(row=r, column=20, value=d_inv_val)
                    ws.cell(row=r, column=21, value=d_row[2] or "")
                    ws.cell(row=r, column=22, value=d_row[3] or "")
                    ws.cell(row=r, column=23, value=d_row[4] or 0)
                    ws.cell(row=r, column=24, value=d_row[5] or 0)

                ws.cell(row=r, column=13, value=p_inv_clean)
                ws.cell(row=r, column=14, value=clean_invoice_text(d_inv_val))
                ws.cell(row=r, column=15, value=f"=M{r}=N{r}") 
                ws.cell(row=r, column=16, value=f"=AND(MONTH(F{r})=MONTH(S{r}), YEAR(F{r})=YEAR(S{r}))")
                ws.cell(row=r, column=17, value=f'=U{r}="{user_vatin_safe}"')
                ws.cell(row=r, column=18, value=f"=G{r}-W{r}")

                # Apply Styles to the new row
                for c in range(1, 25):
                    cell = ws.cell(row=r, column=c)
                    s = style_source[c]
                    cell.font = copy(s['font'])
                    cell.border = copy(s['border'])
                    cell.fill = copy(s['fill'])
                    cell.number_format = s['number_format']
                    cell.alignment = copy(s['alignment'])
                    cell.protection = copy(s['protection'])

        process_sheet('AnnexIII-Local Pur', local_purchases)
        process_sheet('AnnexIII-Import', import_purchases)

        save_dir = os.path.join(settings.MEDIA_ROOT, 'temp_reports')
        os.makedirs(save_dir, exist_ok=True)
        filename = f"AnnexIII_{ovatr_code}.xlsx"
        wb.save(os.path.join(save_dir, filename))
        
        return JsonResponse({'status': 'success', 'redirect_url': f"/crosscheck/results/?ovatr_code={ovatr_code}"})

    except Exception as e: return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    finally:
        if conn: conn.close()

def get_results_data(request):
    """
    UPDATED:
    1. Handles 'page_size' parameter (defaults to 500).
    2. Returns pagination metadata (total pages, current page, total rows).
    3. Handles date matching logic and TIN matching logic.
    """
    ovatr_code = request.GET.get('ovatr_code')
    table_type = request.GET.get('table_type', 'local')
    page = int(request.GET.get('page', 1))
    # NEW: Handle dynamic page size
    page_size = int(request.GET.get('page_size', 500))
    
    if not ovatr_code: return JsonResponse({'status': 'error'}, status=400)
    
    file_path = os.path.join(settings.MEDIA_ROOT, 'temp_reports', f"AnnexIII_{ovatr_code}.xlsx")
    if not os.path.exists(file_path): return JsonResponse({'status': 'error', 'message': 'Not found'}, status=404)

    try:
        conn = get_db_connection()
        # Fetch History
        hist = {r[0] for r in conn.execute("SELECT DISTINCT row_no FROM change_history WHERE ovatr = ?", [ovatr_code]).fetchall()}
        
        # Fetch User VATIN for UI Comparison
        vatin_row = conn.execute("SELECT vatin FROM company_info WHERE ovatr = ?", [ovatr_code]).fetchone()
        user_vatin_clean = clean_invoice_text(vatin_row[0]) if vatin_row else ""
        conn.close()

        sheet_name = 'AnnexIII-Import' if table_type == 'import' else 'AnnexIII-Local Pur'
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, skiprows=8)
        
        if df.empty: return JsonResponse({'status': 'success', 'data': [], 'stats': {'total':0}, 'pagination': {'total_pages': 0, 'current_page': 1}})
        
        df[0] = pd.to_numeric(df[0], errors='coerce')
        df = df.sort_values(by=0)

        p_amt = df[6].apply(clean_currency)
        d_amt = df[22].apply(clean_currency)
        has_p = (df[4].fillna('').astype(str).str.strip() != '') | (p_amt != 0)
        has_d = (df[19].fillna('').astype(str).str.strip() != '') | (d_amt != 0)
        
        valid = has_p | has_d
        status = pd.Series('UNKNOWN', index=df.index)
        status[has_p & ~has_d] = 'NOT FOUND'
        status[has_p & has_d & (abs(p_amt - d_amt) < 0.05)] = 'MATCHED'
        status[has_p & has_d & (abs(p_amt - d_amt) >= 0.05)] = 'MISMATCH'
        
        total_rows = int(valid.sum())
        stats = {
            'total': total_rows, 'matched': int(status[valid].value_counts().get('MATCHED', 0)),
            'not_found': int(status[valid].value_counts().get('NOT FOUND', 0)), 'mismatch': int(status[valid].value_counts().get('MISMATCH', 0))
        }

        # Pagination Logic
        start = (page - 1) * page_size
        end = start + page_size
        df_page = df[valid].iloc[start:end]
        status_page = status[valid].iloc[start:end]
        
        # Calculate Total Pages
        total_pages = (total_rows + page_size - 1) // page_size

        # Helper for Date Comparison
        def check_date_match(v1, v2):
            try:
                if pd.isna(v1) or pd.isna(v2): return False
                if str(v1).strip() == "" or str(v2).strip() == "": return False
                dt1 = pd.to_datetime(v1, dayfirst=True, errors='coerce')
                dt2 = pd.to_datetime(v2, dayfirst=True, errors='coerce')
                if pd.isna(dt1) or pd.isna(dt2): return False
                return dt1.month == dt2.month and dt1.year == dt2.year
            except: return False

        results = []
        for (idx, row), st in zip(df_page.iterrows(), status_page):
            def val(c): return str(row[c]).strip() if pd.notna(row[c]) else ""
            def num(c): return clean_currency(row[c])
            def cl_dt(v):
                if pd.isna(v) or str(v).strip() == "": return ""
                try: return pd.to_datetime(v, dayfirst=True).strftime('%d-%m-%Y')
                except: return str(v).split(' ')[0]
            
            p_clean = clean_invoice_text(val(4))
            d_clean = clean_invoice_text(val(19))
            
            results.append({
                'no': val(0), 'has_history': val(0) in hist, 'status': st,
                'p_inv_clean': p_clean, 'd_inv_clean': d_clean,
                'v_inv': (p_clean == d_clean),
                'v_tin': (clean_invoice_text(val(20)) == user_vatin_clean),
                'v_date': check_date_match(row[5], row[18]),
                'v_diff': num(6) - num(22),
                'p_desc': val(1), 'p_supp': val(2), 'p_tin': val(3), 'p_inv': val(4), 'p_date': cl_dt(row[5]), 'p_amt': num(6),
                'd_date': cl_dt(row[18]), 'd_inv': val(19), 'd_tin': val(20), 'd_name': val(21), 'd_amt': num(22)
            })
            
        return JsonResponse({
            'status': 'success', 
            'data': results, 
            'stats': stats, 
            'pagination': {
                'current_page': page,
                'total_pages': total_pages,
                'page_size': page_size,
                'total_rows': total_rows
            }
        })
    except Exception as e: return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def download_report(request):
    """
    Serves the generated Excel file for download.
    """
    ovatr_code = request.GET.get('ovatr_code')
    if not ovatr_code:
        return JsonResponse({'status': 'error', 'message': 'Missing Session ID'}, status=400)
    
    file_path = os.path.join(settings.MEDIA_ROOT, 'temp_reports', f"AnnexIII_{ovatr_code}.xlsx")
    
    if os.path.exists(file_path):
        response = FileResponse(open(file_path, 'rb'), as_attachment=True, filename=f"AnnexIII_{ovatr_code}.xlsx")
        return response
    else:
        return JsonResponse({'status': 'error', 'message': 'File not found'}, status=404)
    
def get_report_data(request):
    """
    Fetches data for the specific sheet requested.
    Sheets: company, annex_1 (state), annex_2 (non-state), annex_3 (local pur), 
            annex_4 (export), annex_5 (local sale), taxpaid
    """
    try:
        ovatr = request.GET.get('ovatr_code')
        sheet = request.GET.get('sheet')
        
        if not ovatr or not sheet:
            return JsonResponse({'status': 'error', 'message': 'Missing parameters'})
            
        con = get_db_connection()
        data = []
        columns = []
        
        if sheet == 'company':
            # Pivot Company Info for Key-Value editing
            row = con.execute("SELECT * FROM company_info WHERE ovatr = ?", [ovatr]).fetchone()
            if row:
                # Get column names
                cols = [desc[0] for desc in con.description]
                for i, col_name in enumerate(cols):
                    if col_name == 'ovatr': continue
                    data.append({'key': col_name, 'value': row[i]})
                columns = [{'key': 'key', 'label': 'Field'}, {'key': 'value', 'label': 'Value'}]
                
        elif sheet == 'annex_1': # State Charge (Imports)
            # Assumption: Type='Import' and state_charge > 0 or specific logic
            res = con.execute("""
                SELECT no, date, invoice_no, supplier_name, total_amount, state_charge 
                FROM purchase 
                WHERE ovatr = ? AND "import" > 0 AND state_charge <> 0
                ORDER BY CAST(no AS INTEGER)
            """, [ovatr])
            cols = [desc[0] for desc in con.description]
            data = [dict(zip(cols, r)) for r in res.fetchall()]
            columns = [{'key': c, 'label': c.replace('_', ' ').title()} for c in cols]
            
        elif sheet == 'annex_2': # Non-State Charge (Imports)
            res = con.execute("""
                SELECT no, date, invoice_no, supplier_name, total_amount, non_state_charge 
                FROM purchase 
                WHERE ovatr = ? AND "import" > 0 AND non_state_charge <> 0
                ORDER BY CAST(no AS INTEGER)
            """, [ovatr])
            cols = [desc[0] for desc in con.description]
            data = [dict(zip(cols, r)) for r in res.fetchall()]
            columns = [{'key': c, 'label': c.replace('_', ' ').title()} for c in cols]
            
        elif sheet == 'annex_3': # Local Purchase
            res = con.execute("""
                SELECT no, date, invoice_no, supplier_name, supplier_tin, total_amount, purchase as amount
                FROM purchase 
                WHERE ovatr = ? AND purchase > 0
                ORDER BY CAST(no AS INTEGER)
            """, [ovatr])
            cols = [desc[0] for desc in con.description]
            data = [dict(zip(cols, r)) for r in res.fetchall()]
            columns = [{'key': c, 'label': c.replace('_', ' ').title()} for c in cols]
            
        elif sheet == 'annex_4': # Export (Sales)
            res = con.execute("""
                SELECT no, date, invoice_no, buyer_name, total_invoice_amount, vat_export 
                FROM sale 
                WHERE ovatr = ? AND vat_export > 0
                ORDER BY CAST(no AS INTEGER)
            """, [ovatr])
            cols = [desc[0] for desc in con.description]
            data = [dict(zip(cols, r)) for r in res.fetchall()]
            columns = [{'key': c, 'label': c.replace('_', ' ').title()} for c in cols]
            
        elif sheet == 'annex_5': # Local Sales
            res = con.execute("""
                SELECT no, date, invoice_no, buyer_name, total_invoice_amount, vat_local_sale 
                FROM sale 
                WHERE ovatr = ? AND (vat_local_sale > 0 OR non_vat_sales > 0 OR vat_zero_rate > 0)
                ORDER BY CAST(no AS INTEGER)
            """, [ovatr])
            cols = [desc[0] for desc in con.description]
            data = [dict(zip(cols, r)) for r in res.fetchall()]
            columns = [{'key': c, 'label': c.replace('_', ' ').title()} for c in cols]
            
        elif sheet == 'taxpaid':
            res = con.execute("SELECT * FROM tax_paid WHERE ovatr = ?", [ovatr])
            cols = [desc[0] for desc in con.description]
            data = [dict(zip(cols, r)) for r in res.fetchall()]
            columns = [{'key': c, 'label': c.replace('_', ' ').title()} for c in cols]

        con.close()
        return JsonResponse({'status': 'success', 'data': data, 'columns': columns})
        
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
def update_report_cell(request):
    """
    Generic update for any cell in the report tables.
    Payload: { ovatr, sheet, id_field, id_val, field, value, old_value }
    """
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            ovatr = body.get('ovatr')
            sheet = body.get('sheet')
            id_val = body.get('id_val') # Primary Key value (e.g., row 'no' or 'tax_year')
            field = body.get('field')
            value = body.get('value')
            old_value = body.get('old_value')
            
            con = get_db_connection()
            
            # Identify Table & PK based on Sheet
            table_map = {
                'company': {'table': 'company_info', 'pk': 'ovatr'},
                'annex_1': {'table': 'purchase', 'pk': 'no'},
                'annex_2': {'table': 'purchase', 'pk': 'no'},
                'annex_3': {'table': 'purchase', 'pk': 'no'},
                'annex_4': {'table': 'sale', 'pk': 'no'},
                'annex_5': {'table': 'sale', 'pk': 'no'},
                'taxpaid': {'table': 'tax_paid', 'pk': 'description'} # Using description as secondary PK component
            }
            
            config = table_map.get(sheet)
            if not config:
                return JsonResponse({'status': 'error', 'message': 'Invalid sheet'})
                
            table = config['table']
            pk_col = config['pk']
            
            # Special Handling for Company Info (Key-Value structure in UI, Column structure in DB)
            if sheet == 'company':
                # 'id_val' in UI is actually the Field Name (key), 'field' in UI is 'value'
                # We update the column named `id_val`
                db_field = id_val 
                query = f'UPDATE {table} SET "{db_field}" = ? WHERE ovatr = ?'
                params = [value, ovatr]
            elif sheet == 'taxpaid':
                # Composite PK: ovatr + tax_year + description. 
                # Assuming UI passes the unique description or we handle it. 
                # Ideally, we should use a stronger ID. For now, we trust the combination.
                # Taxpaid UI usually sends `id_val` as the 'description' field or a composite.
                # Let's assume id_val passed is the 'description'
                query = f'UPDATE {table} SET "{field}" = ? WHERE ovatr = ? AND description = ?'
                params = [value, ovatr, id_val]
            else:
                # Standard ID based update
                query = f'UPDATE {table} SET "{field}" = ? WHERE ovatr = ? AND "{pk_col}" = ?'
                params = [value, ovatr, id_val]

            con.execute(query, params)
            
            # Log History
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            con.execute("INSERT INTO change_history VALUES (?, ?, ?, ?, ?, ?, ?)", 
                        [timestamp, ovatr, str(id_val), table, field, str(old_value), str(value)])
            
            update_session_metadata(con, ovatr)
            con.close()
            
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def download_full_report(request):
    """
    Generates the Full Excel Report.
    Includes 'Robust Parsing' for malformed DB strings (missing quotes).
    Fixes Case-Sensitivity issue for Sheet Names.
    """
    ovatr_code = request.GET.get('ovatr_code')
    if not ovatr_code: return JsonResponse({'status': 'error', 'message': 'Missing Session ID'}, status=400)
    
    try:
        con = get_db_connection()
        row = con.execute("SELECT * FROM company_info WHERE ovatr = ?", [ovatr_code]).fetchone()
        if not row: return JsonResponse({'status': 'error', 'message': 'Not found'}, status=404)
        cols = [desc[0] for desc in con.description]
        data = dict(zip(cols, row))
        con.close()
        
        # --- Helper: Robust List Parser ---
        def parse_list_field(value):
            if not value: return []
            if isinstance(value, list): return value
            
            s_val = str(value).strip()
            if not s_val: return []

            # 1. Try standard JSON
            try: return json.loads(s_val)
            except: pass

            # 2. Try standard Python Literal
            try: return ast.literal_eval(s_val)
            except: pass

            # 3. Apply Regex Fix for unquoted values
            try:
                # Matches keys that are followed by unquoted text up to a comma or closing brace
                pattern = r"(:\s*)([^'\"\[\{\],\s][^,}]*?)(?=\s*[,}])"
                fixed_val = re.sub(pattern, r"\1'\2'", s_val)
                return ast.literal_eval(fixed_val)
            except Exception as e:
                print(f"DEBUG: Parsing failed completely for: {s_val[:50]}... Error: {e}")
                return []

        # Load Template
        wb = None
        paths = [
            os.path.join(settings.BASE_DIR, 'templates', 'Sample-Excel_Report.xlsx'),
            os.path.join(settings.MEDIA_ROOT, 'templates', 'Sample-Excel_Report.xlsx')
        ]
        for p in paths:
            if os.path.exists(p): wb = load_workbook(p); break
        
        if not wb: return JsonResponse({'status': 'error', 'message': 'Template missing'}, status=500)

        # --- FIX: Case-Insensitive Sheet Finding ---
        ws = None
        target_name = 'Company Information'
        
        # Loop through existing sheets to find a match (ignoring case)
        for sheet_name in wb.sheetnames:
            if sheet_name.strip().lower() == target_name.lower():
                ws = wb[sheet_name]
                break
        
        # If still not found, create it
        if ws is None:
            ws = wb.create_sheet(target_name, 0)

        khmer_font = Font(name='Khmer OS Siemreap', size=11)
        def write_cell(ref, val):
            c = ws[ref]
            c.value = str(val) if val is not None else ""
            c.font = khmer_font

        # --- MAPPING ---
        write_cell('D2', data.get('company_name_kh', ''))
        write_cell('D3', data.get('company_name_en', ''))
        write_cell('D4', data.get('vatin', ''))
        write_cell('D6', data.get('address_main', ''))
        write_cell('D10', data.get('phone', ''))

        # Business Activity
        acts = parse_list_field(data.get('business_activities', []))
        if acts:
            # Prefer 'desc' as per logs showing useful info there, or 'name'
            a = acts[0]
            write_cell('D5', a.get('desc') or a.get('name') or '')

        # Accounts
        accs = parse_list_field(data.get('enterprise_accounts', []))
        if accs:
            acc = accs[0]
            # Mapped: D11 -> number, D12 -> bank
            write_cell('D11', acc.get('number', '')) 
            write_cell('D12', acc.get('bank', ''))   
        
        # Save
        save_dir = os.path.join(settings.MEDIA_ROOT, 'reports')
        os.makedirs(save_dir, exist_ok=True)
        fname = f"FullReport_{ovatr_code}.xlsx"
        full_path = os.path.join(save_dir, fname)
        wb.save(full_path)
        
        return FileResponse(open(full_path, 'rb'), as_attachment=True, filename=fname)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)