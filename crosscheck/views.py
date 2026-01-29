from django.conf import settings
import duckdb
import pandas as pd
import os
import re
import json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.storage import FileSystemStorage

# --- Helper: DuckDB Connection ---
def get_db_connection():
    return duckdb.connect(str(settings.BASE_DIR / 'datawarehouse.duckdb'))

# --- Helper: Currency Cleaner ---
def clean_currency(val):
    s = str(val).strip()
    if s.lower() in ['nan', 'none', '', 'nat', '-']:
        return 0.0
    clean_s = re.sub(r'[^\d.-]', '', s)
    if '(' in s and ')' in s:
        clean_s = '-' + re.sub(r'[^\d.]', '', s)
    try:
        return float(clean_s)
    except ValueError:
        return 0.0

def new_crosscheck(request):
    return render(request, 'crosscheck/new.html')

@csrf_exempt
def upload_init(request):
    if request.method == 'POST' and request.FILES.get('file'):
        file = request.FILES['file']
        fs = FileSystemStorage()
        clean_name = fs.get_available_name(file.name)
        filename = fs.save(os.path.join("temp", clean_name), file)
        uploaded_file_path = fs.path(filename)

        try:
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
            if 'OVATR' not in data or not data['OVATR']:
                return JsonResponse({'status': 'error', 'message': 'Missing Critical Field: OVATR'}, status=400)

            con = get_db_connection()
            columns_schema = []
            for key in data.keys():
                if key == 'OVATR': columns_schema.append(f'"{key}" VARCHAR PRIMARY KEY')
                else: columns_schema.append(f'"{key}" VARCHAR')
            
            con.execute(f"CREATE TABLE IF NOT EXISTS companyInfo ({', '.join(columns_schema)})")
            
            columns = [f'"{k}"' for k in data.keys()]
            placeholders = ['?'] * len(data)
            values = list(data.values())
            con.execute(f"INSERT OR REPLACE INTO companyInfo ({', '.join(columns)}) VALUES ({', '.join(placeholders)})", values)
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
                        'OVATR': body['ovatr'], 'TaxYear': current_year, 'Description': description,
                        'Jan': clean_money(row.values[3]), 'Feb': clean_money(row.values[4]), 'Mar': clean_money(row.values[5]),
                        'Apr': clean_money(row.values[6]), 'May': clean_money(row.values[7]), 'Jun': clean_money(row.values[8]),
                        'Jul': clean_money(row.values[9]), 'Aug': clean_money(row.values[10]), 'Sep': clean_money(row.values[11]),
                        'Oct': clean_money(row.values[12]), 'Nov': clean_money(row.values[13]), 'Dec': clean_money(row.values[14]),
                        'Total': clean_money(row.values[15]),
                    })

            if extracted_rows:
                con = get_db_connection()
                con.execute("CREATE TABLE IF NOT EXISTS taxPaid (OVATR VARCHAR, TaxYear VARCHAR, Description VARCHAR, Jan DOUBLE, Feb DOUBLE, Mar DOUBLE, Apr DOUBLE, May DOUBLE, Jun DOUBLE, Jul DOUBLE, Aug DOUBLE, Sep DOUBLE, Oct DOUBLE, Nov DOUBLE, Dec DOUBLE, Total DOUBLE, PRIMARY KEY (OVATR, TaxYear, Description))")
                con.execute("DELETE FROM taxPaid WHERE OVATR = ?", [body['ovatr']])
                con.executemany("INSERT INTO taxPaid VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [list(d.values()) for d in extracted_rows])
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
            fs = FileSystemStorage()
            try:
                df = pd.read_excel(fs.path(body['temp_path']), sheet_name='PURCHASE', header=None)
            except ValueError:
                return JsonResponse({'status': 'error', 'message': 'Sheet "PURCHASE" not found'}, status=400)

            df = df.iloc[3:] # Skip 3 rows
            if len(df.columns) < 17:
                return JsonResponse({'status': 'error', 'message': f'Format Mismatch: Expected 17 columns (A-Q), found {len(df.columns)}.'})

            target_cols = [
                'Excel_No', 'Date', 'Invoice_No', 'Type', 'Supplier_TIN', 'Supplier_Name', 
                'Total_Amount', 'Exclude_VAT', 'Non_VAT_Purchase', 'VAT_0', 
                'Purchase', 'Import', 'Non_Creditable_VAT', 'State_Charge', 'Non_State_Charge', 
                'Description', 'Status'
            ]
            df = df.iloc[:, :17]; df.columns = target_cols
            df = df[df['Date'].notna()]
            df['No'] = range(1, len(df) + 1); df['No'] = df['No'].astype(str)

            for col in ['Total_Amount', 'Exclude_VAT', 'Non_VAT_Purchase', 'VAT_0', 'Purchase', 'Import', 'Non_Creditable_VAT', 'State_Charge', 'Non_State_Charge']:
                df[col] = df[col].apply(clean_currency)

            df['OVATR'] = body['ovatr']
            
            con = get_db_connection()
            con.execute("DROP TABLE IF EXISTS purchase")
            con.execute("CREATE TABLE purchase (OVATR VARCHAR, No VARCHAR, Date VARCHAR, Invoice_No VARCHAR, Type VARCHAR, Supplier_TIN VARCHAR, Supplier_Name VARCHAR, Total_Amount DOUBLE, Exclude_VAT DOUBLE, Non_VAT_Purchase DOUBLE, VAT_0 DOUBLE, Purchase DOUBLE, Import DOUBLE, Non_Creditable_VAT DOUBLE, State_Charge DOUBLE, Non_State_Charge DOUBLE, Description VARCHAR, Status VARCHAR, PRIMARY KEY (OVATR, No))")
            con.register('df_purchase', df)
            con.execute("INSERT INTO purchase SELECT OVATR, No, Date, Invoice_No, Type, Supplier_TIN, Supplier_Name, Total_Amount, Exclude_VAT, Non_VAT_Purchase, VAT_0, Purchase, Import, Non_Creditable_VAT, State_Charge, Non_State_Charge, Description, Status FROM df_purchase")
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
            fs = FileSystemStorage()
            try:
                df = pd.read_excel(fs.path(body['temp_path']), sheet_name='SALE', header=None)
            except ValueError:
                return JsonResponse({'status': 'error', 'message': 'Sheet "SALE" not found'}, status=400)

            df = df.iloc[3:] # Skip 3 rows
            if len(df.columns) < 23:
                 return JsonResponse({'status': 'error', 'message': f'Format Mismatch: Expected 23+ columns (A-W), found {len(df.columns)}'})

            target_cols = [
                'Excel_No', 'Date', 'Invoice_No', 'Credit_Note_No', 'Buyer_Type', 'Tax_Registration_ID', 
                'Buyer_Name', 'Total_Invoice_Amount', 'Amount_Exclude_VAT', 'Non_VAT_Sales', 
                'VAT_Zero_Rate', 'VAT_Local_Sale', 'VAT_Export', 'VAT_Local_Sale_State_Burden', 
                'VAT_Withheld_By_National_Treasury', 'PLT', 'Special_Tax_On_Goods', 
                'Special_Tax_On_Services', 'Accommodation_Tax', 'Income_Tax_Redemption_Rate', 
                'Notes', 'Description', 'Tax_Declaration_Status'
            ]
            df = df.iloc[:, :23]; df.columns = target_cols
            df = df[df['Date'].notna()]
            df['No'] = range(1, len(df) + 1); df['No'] = df['No'].astype(str)

            numeric_cols = [
                'Total_Invoice_Amount', 'Amount_Exclude_VAT', 'Non_VAT_Sales', 'VAT_Zero_Rate', 
                'VAT_Local_Sale', 'VAT_Export', 'VAT_Local_Sale_State_Burden', 
                'VAT_Withheld_By_National_Treasury', 'PLT', 'Special_Tax_On_Goods', 
                'Special_Tax_On_Services', 'Accommodation_Tax', 'Income_Tax_Redemption_Rate'
            ]
            for col in numeric_cols:
                df[col] = df[col].apply(clean_currency)

            df['OVATR'] = body['ovatr']
            
            con = get_db_connection()
            con.execute("DROP TABLE IF EXISTS sale")
            con.execute("""
                CREATE TABLE sale (
                    OVATR VARCHAR, No VARCHAR, Date VARCHAR, Invoice_No VARCHAR, Credit_Note_No VARCHAR,
                    Buyer_Type VARCHAR, Tax_Registration_ID VARCHAR, Buyer_Name VARCHAR,
                    Total_Invoice_Amount DOUBLE, Amount_Exclude_VAT DOUBLE, Non_VAT_Sales DOUBLE,
                    VAT_Zero_Rate DOUBLE, VAT_Local_Sale DOUBLE, VAT_Export DOUBLE,
                    VAT_Local_Sale_State_Burden DOUBLE, VAT_Withheld_By_National_Treasury DOUBLE, PLT DOUBLE,
                    Special_Tax_On_Goods DOUBLE, Special_Tax_On_Services DOUBLE, Accommodation_Tax DOUBLE,
                    Income_Tax_Redemption_Rate DOUBLE, Notes VARCHAR, Description VARCHAR,
                    Tax_Declaration_Status VARCHAR, PRIMARY KEY (OVATR, No)
                )
            """)
            con.register('df_sale', df)
            con.execute("""
                INSERT INTO sale SELECT 
                    OVATR, No, Date, Invoice_No, Credit_Note_No, Buyer_Type, Tax_Registration_ID, Buyer_Name,
                    Total_Invoice_Amount, Amount_Exclude_VAT, Non_VAT_Sales, VAT_Zero_Rate, VAT_Local_Sale,
                    VAT_Export, VAT_Local_Sale_State_Burden, VAT_Withheld_By_National_Treasury, PLT,
                    Special_Tax_On_Goods, Special_Tax_On_Services, Accommodation_Tax, Income_Tax_Redemption_Rate,
                    Notes, Description, Tax_Declaration_Status
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
                'Excel_No', 'Date', 'Invoice_No', 'Supplier_Non_Resident', 'Supplier_TIN', 
                'Supplier_Name', 'Address', 'Email', 'Non_VAT_Supply', 'Exclude_VAT', 
                'VAT', 'Description', 'Status', 'Declaration_Status'
            ]
            df = df.iloc[:, :14]; df.columns = target_cols
            df = df[df['Date'].notna()]
            df['No'] = range(1, len(df) + 1); df['No'] = df['No'].astype(str)

            for col in ['Non_VAT_Supply', 'Exclude_VAT', 'VAT']:
                df[col] = df[col].apply(clean_currency)

            df['OVATR'] = body['ovatr']
            
            con = get_db_connection()
            con.execute("DROP TABLE IF EXISTS reverse_charge")
            con.execute("""
                CREATE TABLE reverse_charge (
                    OVATR VARCHAR, No VARCHAR, Date VARCHAR, Invoice_No VARCHAR, Supplier_Non_Resident VARCHAR,
                    Supplier_TIN VARCHAR, Supplier_Name VARCHAR, Address VARCHAR, Email VARCHAR,
                    Non_VAT_Supply DOUBLE, Exclude_VAT DOUBLE, VAT DOUBLE, Description VARCHAR,
                    Status VARCHAR, Declaration_Status VARCHAR, PRIMARY KEY (OVATR, No)
                )
            """)
            con.register('df_rc', df)
            con.execute("INSERT INTO reverse_charge SELECT OVATR, No, Date, Invoice_No, Supplier_Non_Resident, Supplier_TIN, Supplier_Name, Address, Email, Non_VAT_Supply, Exclude_VAT, VAT, Description, Status, Declaration_Status FROM df_rc")
            con.close()
            return JsonResponse({'status': 'success', 'message': f'Saved {len(df)} Reverse Charge Records.'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Invalid Method'}, status=405)