import pandas as pd
import os
import re
import json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.storage import FileSystemStorage

def new_crosscheck(request):
    """
    Renders the main interface for the New Crosscheck module.
    """
    return render(request, 'crosscheck/new.html')

@csrf_exempt
def upload_init(request):
    """
    Handles the raw Excel upload.
    """
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
                # A-D Static Fields
                'company_name_kh': '', 'company_name_en': '', 'file_barcode': '',
                'old_vatin': '', 'vatin': '', 'enterprise_id': '',
                'registered_entity': '', 'reg_date': '', 'success_date': '', 
                'taxpayer_type': '', 'status': '', 'tax_year': '',
                'address_main': '', 'address_office': '', 'phone': '', 'email': '',
                'employee_count': '', 'total_salary': '', 'property_type': '',
                'rent_per_month': '', 'enterprise_form': '', 'add_ent_form': '', 'signage': '',
                
                # H. Estimates (Explicit Keys)
                'h_date': '', 
                'h_real_12m': '', 'h_real_3m': '',
                'h_est_12m': '', 'h_est_3m': '',

                # Dynamic Tables
                'business_activities': [], 'enterprise_accounts': [], 'related_institutions': []
            }

            # --- Helpers ---
            def get_val_safe(val):
                """Clean individual cell values"""
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
            estimate_header_index = None # Track where Section H starts

            # --- 1. Main Loop (A-G and Section Detection) ---
            for index, row in df.iterrows():
                cell_0 = get_col(row, 0)
                
                # Detect H Section Start
                if "ការប៉ាន់ស្មានផលរបរ" in cell_0:
                    estimate_header_index = index
                    # Don't continue, we might need to parse dynamic tables below if mixed layout
                
                # A-D Static Parsing
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

                # Dynamic Tables Detection
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

            # --- 2. Post-Loop Extraction for H (Positional Logic) ---
            if estimate_header_index is not None:
                i = estimate_header_index
                
                # Helper to grab cell by (row_index, col_index)
                def get_cell(r, c):
                    if r < len(df) and c < len(df.columns):
                        return get_val_safe(df.iat[r, c])
                    return ""

                # Step 2: Date is 2 rows down, Column C (index 2)
                # Header @ i => Date @ i+2, Col C
                data_map['h_date'] = get_cell(i + 2, 2)

                # Step 3: Real 12m is 2 more rows down (i+4), Column C (index 2)
                #         Real 3m is 2 more rows down (i+4), Column D (index 3)
                data_map['h_real_12m'] = get_cell(i + 4, 2)
                data_map['h_real_3m'] = get_cell(i + 4, 3)

                # Step 4: Est 12m is 1 more row down (i+5), Column C (index 2)
                #         Est 3m is 1 more row down (i+5), Column D (index 3)
                data_map['h_est_12m'] = get_cell(i + 5, 2)
                data_map['h_est_3m'] = get_cell(i + 5, 3)

            return JsonResponse({'status': 'success', 'data': data_map, 'temp_path': filename})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})
            
    return JsonResponse({'status': 'error', 'message': 'No file provided'})