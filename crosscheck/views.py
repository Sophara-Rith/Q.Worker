import glob
import os
import io
import json
import re
import time
import duckdb
import warnings
import pandas as pd
from datetime import datetime
from django.conf import settings
from django.shortcuts import render
from django.http import FileResponse, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.storage import FileSystemStorage
from openpyxl import load_workbook

# --- Helpers ---

def get_db_connection():
    return duckdb.connect(os.path.join(settings.BASE_DIR, 'datawarehouse.duckdb'))

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
                        data_map['enterprise_accounts'].append({
                            'no': get_col(row, 1), 'bank': get_col(row, 2), 
                            'account_name': get_col(row, 3), 'number': get_col(row, 4), 
                            'currency': get_col(row, 5), 'type': get_col(row, 6)
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

            # Store OVATR in session
            request.session['ovatr_code'] = clean_data['ovatr']

            con = get_db_connection()
            columns_schema = []
            for key in clean_data.keys():
                if key == 'ovatr': columns_schema.append(f'"{key}" VARCHAR PRIMARY KEY')
                else: columns_schema.append(f'"{key}" VARCHAR')
            
            con.execute(f"CREATE TABLE IF NOT EXISTS company_info ({', '.join(columns_schema)})")
            
            columns = [f'"{k}"' for k in clean_data.keys()]
            placeholders = ['?'] * len(clean_data)
            values = list(clean_data.values())
            con.execute(f"INSERT OR REPLACE INTO company_info ({', '.join(columns)}) VALUES ({', '.join(placeholders)})", values)
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

def get_crosscheck_stats(request):
    """ Returns data stats including both Local and Import counts. """
    ovatr_code = request.GET.get('ovatr_code') or request.session.get('ovatr_code')
    if not ovatr_code: return JsonResponse({'status': 'error'}, status=400)

    try:
        conn = get_db_connection()
        
        # Count Local Purchase (>0) and Import (>0)
        res_p = conn.execute("""
            SELECT 
                COUNT(CASE WHEN purchase > 0 THEN 1 END),
                COUNT(CASE WHEN "import" > 0 THEN 1 END)
            FROM purchase WHERE ovatr = ?
        """, [ovatr_code]).fetchone()
        
        count_local = res_p[0] if res_p else 0
        count_import = res_p[1] if res_p else 0
        
        # Declarations (Matched via Strict Logic)
        res_d = conn.execute("""
            SELECT COUNT(DISTINCT d.invoice_number)
            FROM tax_declaration d
            JOIN purchase p ON d.invoice_number = p.invoice_no
            WHERE p.ovatr = ?
            AND d.tax_registration_id = p.supplier_tin
            AND month(d.date) = month(strptime(p.date, '%d-%m-%Y'))
            AND year(d.date) = year(strptime(p.date, '%d-%m-%Y'))
        """, [ovatr_code]).fetchone()
        count_d = res_d[0] if res_d else 0
        
        conn.close()
        file_path = os.path.join(settings.MEDIA_ROOT, 'temp_reports', f"AnnexIII_{ovatr_code}.xlsx")
        
        return JsonResponse({
            'status': 'success',
            'total_rows': max(count_local + count_import, count_d),
            'local_count': count_local,
            'import_count': count_import,
            'declaration_count': count_d,
            'is_ready': os.path.exists(file_path)
        })
    except Exception as e: return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def generate_annex_iii(request):
    """ 
    Generates Excel with TWO sheets:
    1. 'AnnexIII-Local Pur' (purchase > 0)
    2. 'AnnexIII-Import' (import > 0)
    """
    conn = None
    try:
        template_path = os.path.join(settings.BASE_DIR, 'core', 'templates', 'static', 'CC - guide.xlsx')
        if not os.path.exists(template_path):
            template_path = os.path.join(settings.BASE_DIR, 'core', 'static', 'CC - guide.xlsx')
        
        ovatr_code = request.GET.get('ovatr_code') or request.session.get('ovatr_code')
        if not ovatr_code:
            return JsonResponse({'status': 'error', 'message': 'Session ID missing'}, status=400)

        conn = get_db_connection()

        # --- DATA FETCHING ---
        
        # 1. Local Purchases
        local_purchases = conn.execute("""
            SELECT description, supplier_name, supplier_tin, invoice_no, date, purchase 
            FROM purchase WHERE ovatr = ? AND purchase > 0
        """, [ovatr_code]).fetchall()

        # 2. Import Purchases (using "import" column)
        import_purchases = conn.execute("""
            SELECT description, supplier_name, supplier_tin, invoice_no, date, "import"
            FROM purchase WHERE ovatr = ? AND "import" > 0
        """, [ovatr_code]).fetchall()

        # 3. Declarations (Strict Match)
        raw_decs = conn.execute("""
            SELECT d.date, d.invoice_number, d.tax_registration_id, d.buyer_name, d.vat_local_sale, d.vat_export 
            FROM tax_declaration d
            JOIN purchase p ON d.invoice_number = p.invoice_no
            WHERE p.ovatr = ?
            AND d.tax_registration_id = p.supplier_tin
            AND month(d.date) = month(strptime(p.date, '%d-%m-%Y'))
            AND year(d.date) = year(strptime(p.date, '%d-%m-%Y'))
        """, [ovatr_code]).fetchall()
        
        dec_map = {}
        for dec in raw_decs:
            clean_inv_key = clean_invoice_text(dec[1]) 
            if clean_inv_key: dec_map[clean_inv_key] = dec

        company_vatin = ""
        try:
            res = conn.execute("SELECT vatin FROM company_info WHERE ovatr = ? LIMIT 1", [ovatr_code]).fetchone()
            if res and res[0]: company_vatin = res[0]
        except: pass

        # --- EXCEL GENERATION ---
        
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            wb = load_workbook(template_path)
        
        # Helper to process a sheet
        def process_sheet(sheet_name, data_rows):
            if sheet_name not in wb.sheetnames: return # Skip if template missing sheet
            ws = wb[sheet_name]
            
            # Find header start
            start_row = 8
            for r in range(1, 15):
                if ws.cell(row=r, column=1).value and "ល.រ" in str(ws.cell(row=r, column=1).value):
                    start_row = r + 1
                    break

            for i, p_row in enumerate(data_rows):
                r = start_row + i
                p_inv_val = p_row[3] if p_row[3] else ""
                p_inv_clean = clean_invoice_text(p_inv_val)

                # Purchase Data
                ws.cell(row=r, column=1, value=i+1)
                ws.cell(row=r, column=2, value=p_row[0] or "")
                ws.cell(row=r, column=3, value=p_row[1] or "")
                ws.cell(row=r, column=4, value=p_row[2] or "")
                ws.cell(row=r, column=5, value=p_inv_val)
                ws.cell(row=r, column=6, value=p_row[4] or "")
                ws.cell(row=r, column=7, value=p_row[5] if p_row[5] else 0)

                ws.cell(row=r, column=9, value=f"=G{r}")
                ws.cell(row=r, column=11, value=f"=I{r}-G{r}")

                # Dec Match
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
                ws.cell(row=r, column=17, value=f'=C{r}=U{r}')
                ws.cell(row=r, column=18, value=f"=G{r}-W{r}")

        # Process Both Sheets
        process_sheet('AnnexIII-Local Pur', local_purchases)
        
        # Attempt to process Imports (Check if sheet exists, fallback to creating or using 2nd sheet)
        import_sheet_name = 'AnnexIII-Import'
        if import_sheet_name not in wb.sheetnames:
            # Fallback: Copy Local Pur sheet logic if Import sheet missing in template
            if 'AnnexIII-Local Pur' in wb.sheetnames:
                target = wb.copy_worksheet(wb['AnnexIII-Local Pur'])
                target.title = import_sheet_name
        
        process_sheet(import_sheet_name, import_purchases)

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
    API to read the Excel file. 
    Supports 'table_type' param: 'local' (default) or 'import'.
    """
    ovatr_code = request.GET.get('ovatr_code') or request.session.get('ovatr_code')
    table_type = request.GET.get('table_type', 'local') # 'local' or 'import'
    page = int(request.GET.get('page', 1))
    page_size = 100
    
    if not ovatr_code: return JsonResponse({'status': 'error', 'message': 'Missing Session ID'}, status=400)
    file_path = os.path.join(settings.MEDIA_ROOT, 'temp_reports', f"AnnexIII_{ovatr_code}.xlsx")
    if not os.path.exists(file_path): return JsonResponse({'status': 'error', 'message': 'Report not found.'}, status=404)

    try:
        # Determine Sheet
        sheet_name = 'AnnexIII-Import' if table_type == 'import' else 'AnnexIII-Local Pur'
        
        # Read Excel
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, skiprows=8)
        except ValueError:
            # Sheet might not exist if 0 imports
            return JsonResponse({'status': 'success', 'data': [], 'stats': {'total': 0}, 'has_more': False})

        if df.shape[1] < 23: return JsonResponse({'status': 'error', 'message': 'Format mismatch'}, status=500)

        p_inv = df[4].fillna('').astype(str).str.strip()
        d_inv = df[19].fillna('').astype(str).str.strip()
        p_amt = df[6].apply(clean_currency) 
        d_amt = df[22].apply(clean_currency)
        
        has_p = (p_inv != '') | (p_amt != 0)
        has_d = (d_inv != '') | (d_amt != 0)
        valid_mask = has_p | has_d
        
        status_series = pd.Series('UNKNOWN', index=df.index)
        cond_match = has_p & has_d & (abs(p_amt - d_amt) < 0.05)
        cond_mismatch = has_p & has_d & (abs(p_amt - d_amt) >= 0.05)
        status_series[has_p & ~has_d] = 'NOT FOUND'
        status_series[cond_match] = 'MATCHED'
        status_series[cond_mismatch] = 'MISMATCH'
        
        counts = status_series[valid_mask].value_counts()
        stats = {
            'total': int(valid_mask.sum()), 'matched': int(counts.get('MATCHED', 0)),
            'not_found': int(counts.get('NOT FOUND', 0)), 'mismatch': int(counts.get('MISMATCH', 0))
        }

        df_display = df[valid_mask]
        status_display = status_series[valid_mask]
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        df_page = df_display.iloc[start_idx:end_idx]
        status_page = status_display.iloc[start_idx:end_idx]
        
        results = []
        for (idx, row), status in zip(df_page.iterrows(), status_page):
            def val(c): return str(row[c]).strip() if (c < len(row) and pd.notna(row[c])) else ""
            def num(c): return clean_currency(row[c]) if c < len(row) else 0.0
            
            p_inv_clean = clean_invoice_text(val(4))
            d_inv_clean = clean_invoice_text(val(19))
            p_tin_clean = clean_invoice_text(val(3))
            d_tin_clean = clean_invoice_text(val(20))

            v_inv_match = (p_inv_clean == d_inv_clean) if (p_inv_clean and d_inv_clean) else False
            v_tin_match = (p_tin_clean == d_tin_clean) if (p_tin_clean and d_tin_clean) else False
            
            v_date_match = False
            try:
                pd_dt = pd.to_datetime(row[5], dayfirst=True)
                dd_dt = pd.to_datetime(row[18], dayfirst=True)
                if pd.notna(pd_dt) and pd.notna(dd_dt):
                    v_date_match = (pd_dt.month == dd_dt.month) and (pd_dt.year == dd_dt.year)
            except: pass

            v_diff = num(6) - num(22)
            def clean_date(v):
                if pd.isna(v) or str(v).strip() == "": return ""
                try: return pd.to_datetime(v, dayfirst=True).strftime('%d-%m-%Y')
                except: return str(v).split(' ')[0]

            results.append({
                'no': idx + 1, 'status': status,
                'p_inv_clean': p_inv_clean, 'd_inv_clean': d_inv_clean,
                'v_inv': v_inv_match, 'v_date': v_date_match, 'v_tin': v_tin_match, 'v_diff': v_diff,
                'p_desc': val(1), 'p_supp': val(2), 'p_tin': val(3), 'p_inv': val(4), 'p_date': clean_date(row[5]), 'p_amt': num(6),
                'd_date': clean_date(row[18]), 'd_inv': val(19), 'd_tin': val(20), 'd_name': val(21), 'd_amt': num(22)
            })
            
        return JsonResponse({'status': 'success', 'data': results, 'stats': stats, 'has_more': end_idx < stats['total']})

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