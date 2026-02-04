import os
import shutil
import logging
import re
import traceback
import uuid
import pandas as pd
import duckdb
import patoolib
import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter
from datetime import datetime
from django.conf import settings
from core.models import UserSettings, Notification # Imported Notification

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BASE_DIR = settings.BASE_DIR
DB_PATH = os.path.join(BASE_DIR, 'dataWarehouse.duckdb')

# --- SPECIAL CONFIG ---
# TINs that require splitting by single year (Chunk Size = 1)
TINS_SPLIT_BY_YEAR = [
    "L001-100044638",
    # Add other special TINs here
]

class ProgressTracker:
    _instances = {}

    @classmethod
    def update(cls, task_id, progress, status, details=""):
        cls._instances[task_id] = {
            'progress': progress,
            'status': status,
            'details': details
        }

    @classmethod
    def get(cls, task_id):
        return cls._instances.get(task_id, {'progress': 0, 'status': 'Pending', 'details': ''})

class ConsolidationService:
    def __init__(self, task_id, user):
            self.task_id = task_id
            self.user = user
            self.con = duckdb.connect(DB_PATH)
            
            # --- DYNAMIC OUTPUT DIRECTORY (From Core) ---
            try:
                # Use the related_name 'settings' or query directly
                user_settings = UserSettings.objects.get(user=user)
                self.output_dir = user_settings.default_output_dir
            except UserSettings.DoesNotExist:
                # Fallback
                if os.path.exists("D:/"):
                    self.output_dir = "D:/QWorker_Output"
                else:
                    self.output_dir = os.path.join(os.path.expanduser("~"), "QWorker_Output")
            
            os.makedirs(self.output_dir, exist_ok=True)
            self.archive_dir = os.path.join(self.output_dir, 'Archive')
            
            self.init_db()

    def init_db(self):
        """Init DB with EXACT schema."""
        self.con.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_tax_id;
            CREATE TABLE IF NOT EXISTS tax_declaration (
                id INTEGER DEFAULT nextval('seq_tax_id'),
                date DATE,
                invoice_number VARCHAR,
                credit_notification_letter_number VARCHAR,
                buyer_type VARCHAR,
                tax_registration_id VARCHAR,
                buyer_name VARCHAR,
                total_invoice_amount DECIMAL(15,2),
                amount_exclude_vat DECIMAL(15,2),
                non_vat_sales DECIMAL(15,2),
                vat_zero_rate DECIMAL(15,2),
                vat_local_sale DECIMAL(15,2),
                vat_export DECIMAL(15,2),
                vat_local_sale_state_burden DECIMAL(15,2),
                vat_withheld_by_national_treasury DECIMAL(15,2),
                plt DECIMAL(15,2),
                special_tax_on_goods DECIMAL(15,2),
                special_tax_on_services DECIMAL(15,2),
                accommodation_tax DECIMAL(15,2),
                income_tax_redemption_rate DECIMAL(15,2),
                notes VARCHAR,
                description VARCHAR,
                tax_declaration_status VARCHAR,
                telegram_username VARCHAR,
                import_timestamp TIMESTAMP,
                file_tin VARCHAR,
                branch_name VARCHAR
            );
        """)
        try:
            self.con.execute("SELECT branch_name FROM tax_declaration LIMIT 1")
        except:
            logger.warning("Schema mismatch detected. Recreating table.")
            self.con.execute("DROP TABLE IF EXISTS tax_declaration")
            self.init_db()

    def log(self, percent, status, details):
        ProgressTracker.update(self.task_id, percent, status, details)

    def extract_branch_info(self, original_title):
        if not original_title or not isinstance(original_title, str): return ""
        location_match = re.search(r'\(([^)]*ទីស្នាក់ការកណ្តាល[^)]*)\)', original_title)
        if location_match: return location_match.group(1).strip()
        branch_match = re.search(r'\(([^)]*សាខា[^)]*)\)', original_title)
        if branch_match: return branch_match.group(1).strip()
        return ''

    def clean_company_name(self, original_title):
        if not original_title: return ""
        company_match = re.search(r'របស់\s*([^(]+(?:\s*\([^)]*\))?)', original_title)
        if company_match:
            company_name = company_match.group(1).strip()
            company_name = re.sub(r'\s*\((?:សាខា[^)]*|ទីស្នាក់ការកណ្តាល[^)]*)\)', '', company_name).strip()
            return company_name
        parts = original_title.split('របស់')
        if len(parts) > 1: return parts[-1].strip()
        return original_title

    def get_month_name(self, month_num):
        months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        return months.get(int(month_num), str(month_num))

    def format_month_list(self, month_nums):
        """Convert list of month numbers [1, 2, 3, 6] to 'Jan-Mar, Jun'"""
        if not month_nums: return ""
        nums = sorted(list(set(month_nums)))
        ranges = []
        range_start = nums[0]
        prev = nums[0]
        
        for curr in nums[1:]:
            if curr == prev + 1:
                prev = curr
            else:
                if range_start == prev: ranges.append(self.get_month_name(range_start))
                else: ranges.append(f"{self.get_month_name(range_start)}-{self.get_month_name(prev)}")
                range_start = curr
                prev = curr
        
        if range_start == prev: ranges.append(self.get_month_name(range_start))
        else: ranges.append(f"{self.get_month_name(range_start)}-{self.get_month_name(prev)}")
        return " , ".join(ranges)

    def process(self, file_paths):
        try:
            self.log(5, "Initializing", "Preparing files...")
            extracted_files = []

            # 1. EXTRACT
            for f_path in file_paths:
                f_name = os.path.basename(f_path)
                if f_path.lower().endswith(('.rar', '.zip')):
                    self.log(10, "Extracting", f"Extracting {f_name}...")
                    extract_to = os.path.join(BASE_DIR, 'temp_extract', str(uuid.uuid4()))
                    os.makedirs(extract_to, exist_ok=True)
                    try:
                        patoolib.extract_archive(f_path, outdir=extract_to)
                        for root, _, files in os.walk(extract_to):
                            for file in files:
                                if file.lower().endswith(('.xlsx', '.xls')) and 'SALE' in file.upper():
                                    extracted_files.append(os.path.join(root, file))
                    except Exception as e:
                         logger.error(f"Extraction failed: {e}")
                elif f_path.lower().endswith(('.xlsx', '.xls')):
                    extracted_files.append(f_path)

            # 2. IMPORT
            total = len(extracted_files)
            if total == 0:
                self.log(100, "Error", "No valid SALE files found.")
                return

            self.log(20, "Importing", f"Found {total} files. Importing...")
            
            tins_processed = set()
            tin_company_map = {} 
            
            for i, file in enumerate(extracted_files):
                fname = os.path.basename(file)
                percent = 20 + int((i / total) * 30)
                self.log(percent, "Importing", f"Reading {fname}")
                
                match = re.match(r'^([LKB]\d{3}-\d{9})_', fname)
                tin = match.group(1) if match else "UNKNOWN"
                tins_processed.add(tin)

                branch_info = ""
                try:
                    wb_temp = openpyxl.load_workbook(file, read_only=True, data_only=True)
                    ws_temp = wb_temp.active
                    title_val = ws_temp['A1'].value
                    
                    if title_val and isinstance(title_val, str):
                        if tin not in tin_company_map:
                            cleaned_name = self.clean_company_name(title_val)
                            tin_company_map[tin] = cleaned_name
                        branch_info = self.extract_branch_info(title_val)
                    wb_temp.close()
                except Exception as e:
                    logger.warning(f"Could not read title from {fname}: {e}")

                try:
                    df = pd.read_excel(file, skiprows=3, dtype=str)
                    df.columns = [f'Column{i}' for i in range(len(df.columns))]
                    safe_branch = branch_info.replace("'", "''")
                    
                    # Register dataframe as a view for SQL operations
                    self.con.register('df_view', df)

                    # List of core fields to check for duplicates
                    # These are the fields that must be unique to consider a row "new"
                    core_fields_str = """
                        date, invoice_number, credit_notification_letter_number, buyer_type,
                        tax_registration_id, buyer_name, total_invoice_amount, 
                        amount_exclude_vat, non_vat_sales, vat_zero_rate, vat_local_sale,
                        vat_export, vat_local_sale_state_burden, vat_withheld_by_national_treasury,
                        plt, special_tax_on_goods, special_tax_on_services, accommodation_tax,
                        income_tax_redemption_rate, notes, description, tax_declaration_status
                    """

                    # Execute Insert with EXCEPT logic to skip duplicates
                    self.con.execute(f"""
                        INSERT INTO tax_declaration 
                        (
                            {core_fields_str},
                            file_tin, import_timestamp, telegram_username, branch_name
                        )
                        SELECT 
                            *,
                            '{tin}', now(), '{self.user.username}', '{safe_branch}'
                        FROM (
                            SELECT 
                                TRY_CAST(strptime(Column1, '%d-%m-%Y') AS DATE) as date,
                                Column2 as invoice_number, Column3 as credit_notification_letter_number, Column4 as buyer_type,
                                Column5 as tax_registration_id, Column6 as buyer_name, 
                                TRY_CAST(Column7 AS DECIMAL(15,2)) as total_invoice_amount,
                                TRY_CAST(Column8 AS DECIMAL(15,2)) as amount_exclude_vat,
                                TRY_CAST(Column9 AS DECIMAL(15,2)) as non_vat_sales,
                                TRY_CAST(Column10 AS DECIMAL(15,2)) as vat_zero_rate,
                                TRY_CAST(Column11 AS DECIMAL(15,2)) as vat_local_sale,
                                TRY_CAST(Column12 AS DECIMAL(15,2)) as vat_export,
                                TRY_CAST(Column13 AS DECIMAL(15,2)) as vat_local_sale_state_burden,
                                TRY_CAST(Column14 AS DECIMAL(15,2)) as vat_withheld_by_national_treasury,
                                TRY_CAST(Column15 AS DECIMAL(15,2)) as plt,
                                TRY_CAST(Column16 AS DECIMAL(15,2)) as special_tax_on_goods,
                                TRY_CAST(Column17 AS DECIMAL(15,2)) as special_tax_on_services,
                                TRY_CAST(Column18 AS DECIMAL(15,2)) as accommodation_tax,
                                TRY_CAST(Column19 AS DECIMAL(15,2)) as income_tax_redemption_rate,
                                Column20 as notes, Column21 as description, Column22 as tax_declaration_status
                            FROM df_view
                            WHERE Column2 IS NOT NULL
                            
                            EXCEPT 
                            
                            SELECT {core_fields_str}
                            FROM tax_declaration
                            WHERE file_tin = '{tin}'
                        ) as unique_rows
                    """)
                    
                    self.con.unregister('df_view')
                    
                except Exception as e:
                    logger.error(f"Failed to import {fname}: {e}")
                    continue

            # 3. CONSOLIDATE & EXPORT
            self.log(50, "Consolidating", "Generating Master Files...")
            all_summary_stats = []
            
            for idx, tin in enumerate(tins_processed):
                percent = 50 + int((idx / len(tins_processed)) * 40)
                self.log(percent, "Consolidating", f"Formatting {tin}")
                
                # Fetch All Data for TIN
                df_all = self.con.sql(f"""
                    SELECT * FROM tax_declaration 
                    WHERE file_tin = '{tin}'
                    ORDER BY date ASC
                """).to_df()
                
                if df_all.empty:
                    continue

                # Prepare Stats Logic
                df_all['year'] = df_all['date'].dt.year
                df_all['month'] = df_all['date'].dt.month
                
                # --- STATS COLLECTION ---
                stats_grouped = df_all.groupby('year').agg(
                    count=('id', 'count'),
                    months=('month', lambda x: list(x.unique()))
                ).reset_index()

                for _, row in stats_grouped.iterrows():
                    all_summary_stats.append({
                        "tin": tin,
                        "month_desc": self.format_month_list(row['months']),
                        "year": row['year'],
                        "count": row['count']
                    })

                # --- SPLIT SHEET LOGIC ---
                wb = openpyxl.Workbook()
                wb.remove(wb.active) # Remove default sheet
                
                company_name = tin_company_map.get(tin, tin)
                min_year = int(df_all['year'].min())
                max_year = int(df_all['year'].max())

                if tin in TINS_SPLIT_BY_YEAR:
                    chunk_size = 1
                    logger.info(f"Special TIN {tin}: Splitting by 1 year.")
                else:
                    chunk_size = 4
                
                # Iterate chunks
                current_start = min_year
                while current_start <= max_year:
                    current_end = current_start + chunk_size - 1
                    
                    # Filter Data for this chunk
                    df_chunk = df_all[(df_all['year'] >= current_start) & (df_all['year'] <= current_end)]
                    
                    if not df_chunk.empty:
                        # Determine Sheet Name
                        c_min_date = df_chunk['date'].min()
                        c_max_date = df_chunk['date'].max()
                        sheet_name = f"{c_min_date.strftime('%m-%Y')} - {c_max_date.strftime('%m-%Y')}"
                        
                        # Create and Fill Sheet
                        ws = wb.create_sheet(title=sheet_name)
                        self.write_sheet_data(ws, df_chunk, company_name)
                    
                    current_start += chunk_size
                
                # Save Master File
                master_file = os.path.join(self.output_dir, f"{tin}.xlsx")
                wb.save(master_file)

            # 4. GENERATE SUMMARY REPORT
            self.log(90, "Finalizing", "Updating Summary Report...")
            self.generate_summary_report(all_summary_stats)

            shutil.rmtree(os.path.join(BASE_DIR, 'temp_extract'), ignore_errors=True)
            self.log(100, "Completed", f"Saved to {self.output_dir}")
            
            # --- NOTIFICATION INTEGRATION ---
            if self.user:
                Notification.objects.create(
                    user=self.user,
                    title="Consolidation Finished",
                    message="Data consolidation task completed successfully.",
                    notification_type='SUCCESS'
                )

        except Exception as e:
            logger.error(traceback.format_exc())
            self.log(100, "Failed", str(e))
            # Error Notification
            if self.user:
                Notification.objects.create(
                    user=self.user,
                    title="Consolidation Failed",
                    message=f"Task failed: {str(e)}",
                    notification_type='ERROR'
                )
        finally:
            self.con.close()

    def write_sheet_data(self, ws, df, company_name):
        """Populates a specific worksheet with formatted data."""
        
        # --- STYLES ---
        font_title = Font(name="Khmer OS Muol Light", size=12)
        font_header = Font(name="Khmer OS Siemreap", size=11, bold=True)
        font_body = Font(name="Khmer OS Siemreap", size=11)
        
        thin = Side(style='thin', color="000000")
        light = Side(style='thin', color="DDDDDD")
        border_box = Border(left=thin, right=thin, top=thin, bottom=thin)
        border_light = Border(left=light, right=light, top=light, bottom=light)
        
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=False)
        fill_header = PatternFill(start_color="B0DBBC", end_color="B0DBBC", fill_type="solid")
        fill_alt = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")

        # --- 1. TITLE (Row 1) ---
        title = f"បញ្ជីទិន្នានុប្បវត្តិលក់ របស់ {company_name}"
        ws['A1'] = title
        ws['A1'].font = font_title
        ws.merge_cells('A1:Y1') # Updated merge to Y

        # --- 2. HEADERS (Row 2 & 3) ---
        headers = {
            'A': ("ឆ្នាំ", ""), 
            'B': ("ល.រ", ""), 
            'C': ("កាលបរិច្ឆេទ", ""), 
            'D': ("លេខវិក្កយបត្រ ឬប្រតិវេទគយ", ""), 
            'E': ("លេខលិខិតជូនដំណឹងឥណទាន", ""), 
            'F': ("អ្នកទិញ", "ប្រភេទ"), 
            'G': ("", "លេខសម្គាល់ចុះបញ្ជីពន្ធដា"), 
            'H': ("", "ឈ្មោះ"), 
            'I': ("តម្លៃសរុបលើវិក្កយបត្រ", ""), 
            'J': ("តម្លៃ​​មិន​រួមអតប និងមិនជាប់​អតប​", "តម្លៃមិនរួមអតប"), 
            'K': ("", "ការលក់មិនជាប់អតប"), 
            'L': ("អាករលើតម្លៃបន្ថែមអត្រា ០% ", ""), 
            'M': ("អាករលើតម្លៃបន្ថែម", "ការលក់ក្នុងស្រុក"), 
            'N': ("", "អតបលើការនាំចេញ"), 
            'O': ("អាករលើតម្លៃបន្ថែម(បន្ទុករដ្ឋ)", ""), 
            'P': ("អតប កាត់ទុកដោយរតនាគារជាតិ", ""), 
            'Q': ("អាករបំភ្លឺសាធារណៈ", ""), 
            'R': ("អាករពិសេសលើទំនិញមួយចំនួន", ""), 
            'S': ("អាករពិសេសលើសេវាមួយចំនួន", ""), 
            'T': ("អាករលើការស្នាក់នៅ", ""), 
            'U': ("អត្រាប្រាក់រំដោះពន្ធលើប្រាក់ចំណូល", ""), 
            'V': ("កំណត់សម្គាល់", ""), 
            'W': ("បរិយាយ", ""), 
            'X': ("ស្ថានភាពប្រកាសពន្ធ", ""),
            'Y': ("ស្នាក់ការ", "")
        }

        for col, (p_text, c_text) in headers.items():
            ws[f"{col}2"] = p_text
            if c_text: ws[f"{col}3"] = c_text
            
            for r in [2, 3]:
                cell = ws[f"{col}{r}"]
                cell.font = font_header
                cell.fill = fill_header
                cell.alignment = align_center
                cell.border = border_box

        # --- 3. APPLY MERGES ---
        ws.merge_cells('F2:H2') # Buyer
        ws.merge_cells('J2:K2') # Excl/Non-Tax
        ws.merge_cells('M2:N2') # VAT
        
        vertical_cols = ['A','B','C','D','E','I','L','O','P','Q','R','S','T','U','V','W','X','Y']
        for col in vertical_cols:
            ws.merge_cells(f"{col}2:{col}3")

        # --- 4. DATA WRITING ---
        current_row = 4
        counter = 1
        previous_year = None
        
        # Sort DF by date just in case
        df = df.sort_values('date')

        for _, row in df.iterrows():
            year = row['date'].year if pd.notnull(row['date']) else ""
            
            if previous_year is not None and year != previous_year:
                current_row += 1 
            previous_year = year
            
            vals = [
                year, counter, row['date'], row['invoice_number'], row['credit_notification_letter_number'],
                row['buyer_type'], row['tax_registration_id'], row['buyer_name'], row['total_invoice_amount'],
                row['amount_exclude_vat'], row['non_vat_sales'], row['vat_zero_rate'], row['vat_local_sale'],
                row['vat_export'], row['vat_local_sale_state_burden'], row['vat_withheld_by_national_treasury'],
                row['plt'], row['special_tax_on_goods'], row['special_tax_on_services'], row['accommodation_tax'],
                row['income_tax_redemption_rate'], row['notes'], row['description'], row['tax_declaration_status'], 
                row['branch_name']
            ]
            
            for i, val in enumerate(vals, 1):
                cell = ws.cell(row=current_row, column=i, value=val)
                if i == 3 and val: cell.number_format = 'DD-MM-YYYY'
                if 9 <= i <= 20: cell.number_format = '#,##0' 
                cell.font = font_body
                cell.border = border_light
                if counter % 2 == 0: cell.fill = fill_alt

            current_row += 1
            counter += 1

        # --- 5. WIDTHS ---
        widths = {
            'A': 8, 'B': 6, 'C': 14, 'D': 18, 'E': 15, 'F': 15, 'G': 18, 'H': 30, 
            'I': 20, 'J': 18, 'K': 18, 'L': 18, 'M': 18, 'N': 18, 'O': 18, 
            'P': 18, 'Q': 18, 'R': 18, 'S': 18, 'T': 18, 'U': 15, 'V': 20, 'W': 25, 'X': 20, 'Y': 25
        }
        for col, w in widths.items():
            ws.column_dimensions[col].width = w

    def generate_summary_report(self, summary_data_list):
        """
        Update the master summary Excel file.
        Matches logic from dataConsolidation.py
        """
        if not summary_data_list:
            return None

        try:
            filename = "0.Import_Summary.xlsx"
            filepath = os.path.join(self.output_dir, filename)
            
            # 1. Load existing data if available
            existing_data = []
            if os.path.exists(filepath):
                try:
                    existing_df = pd.read_excel(filepath, sheet_name='Summary')
                    existing_df['TIN'] = existing_df['TIN'].astype(str)
                    existing_df = existing_df[
                        (existing_df['TIN'] != 'nan') & 
                        (existing_df['TIN'] != 'GRAND TOTAL') & 
                        (~existing_df['TIN'].str.contains('Total TINs', na=False)) &
                        (existing_df['TIN'].str.strip() != '')
                    ]
                    existing_data = existing_df.to_dict('records')
                except Exception as e:
                    logger.warning(f"Error reading existing summary: {e}")
                    existing_data = []

            # 2. Merge New Data
            updated_tins = set(str(item['tin']) for item in summary_data_list)
            final_data = [row for row in existing_data if str(row.get('TIN')) not in updated_tins]
            
            for item in summary_data_list:
                final_data.append({
                    'TIN': str(item['tin']),
                    'Month': item['month_desc'],
                    'Year': item['year'],
                    'Total Transactions': item['count']
                })
            
            # 3. Create DataFrame
            df = pd.DataFrame(final_data)
            if df.empty: return None
            
            if 'No' in df.columns: df = df.drop(columns=['No'])

            df['TIN'] = df['TIN'].astype(str)
            df = df.sort_values(by=['TIN', 'Year'])
            df.insert(0, 'No', 0) 
            
            total_transactions = df['Total Transactions'].sum()
            unique_tins_count = df['TIN'].nunique()

            # 4. Write to Excel
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Summary')
                
                worksheet = writer.sheets['Summary']
                
                # --- STYLES ---
                header_font = Font(bold=True, color="FFFFFF")
                header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                thin_border = Side(style='thin', color="000000")
                border = Border(left=thin_border, right=thin_border, top=thin_border, bottom=thin_border)
                center_align = Alignment(horizontal='center', vertical='center')
                left_align = Alignment(horizontal='left', vertical='center', wrap_text=True)
                fill_white = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
                fill_grey = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                
                # Header Style
                for cell in worksheet[1]:
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = center_align
                    cell.border = border

                # Merging & Formatting
                current_tin = None
                start_row = 2
                tin_counter = 1
                use_grey_fill = False 
                
                for row in range(2, worksheet.max_row + 2):
                    cell_value = worksheet.cell(row=row, column=2).value if row <= worksheet.max_row else None
                    
                    if cell_value != current_tin:
                        if current_tin is not None:
                            end_row = row - 1
                            worksheet.cell(row=start_row, column=1).value = tin_counter
                            tin_counter += 1
                            
                            if end_row > start_row:
                                worksheet.merge_cells(start_row=start_row, start_column=1, end_row=end_row, end_column=1)
                                worksheet.merge_cells(start_row=start_row, start_column=2, end_row=end_row, end_column=2)
                            
                            current_fill = fill_grey if use_grey_fill else fill_white
                            for r in range(start_row, end_row + 1):
                                for c in range(1, 6):
                                    cell = worksheet.cell(row=r, column=c)
                                    cell.fill = current_fill
                                    cell.border = border
                                    if c == 3: cell.alignment = left_align
                                    else: cell.alignment = center_align
                                    if c == 5: cell.number_format = '#,##0'

                            use_grey_fill = not use_grey_fill
                        
                        current_tin = cell_value
                        start_row = row

                # Grand Total Row
                last_row = worksheet.max_row + 1
                worksheet.cell(row=last_row, column=1).value = "GRAND TOTAL"
                worksheet.cell(row=last_row, column=2).value = f"Total TINs: {unique_tins_count}"
                worksheet.cell(row=last_row, column=5).value = total_transactions
                
                total_font = Font(bold=True)
                total_fill = PatternFill(start_color="ACB9CA", end_color="ACB9CA", fill_type="solid")
                
                for col in range(1, 6):
                    cell = worksheet.cell(row=last_row, column=col)
                    cell.font = total_font
                    cell.fill = total_fill
                    cell.border = border
                    cell.alignment = center_align
                    if col == 5: cell.number_format = '#,##0'

                worksheet.column_dimensions['A'].width = 15
                worksheet.column_dimensions['B'].width = 25
                worksheet.column_dimensions['C'].width = 35
                worksheet.column_dimensions['D'].width = 10
                worksheet.column_dimensions['E'].width = 20
                
            return filepath
            
        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return None

def run_task(task_id, file_paths, user):
    service = ConsolidationService(task_id, user)
    service.process(file_paths)