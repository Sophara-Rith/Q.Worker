import os
import shutil
import logging
import re
import traceback
import uuid
import pandas as pd
import duckdb
import patoolib
import xlsxwriter
from django.conf import settings
from core.models import UserSettings
from datetime import datetime

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BASE_DIR = settings.BASE_DIR
APPDATA_DIR = os.path.join(os.environ.get('APPDATA', os.path.expanduser('~')), 'AuditCore PRO')
DB_PATH = os.path.join(APPDATA_DIR, 'dataWarehouse.duckdb')

# --- SPECIAL CONFIG ---
# TINs that require splitting by single year (Chunk Size = 1)
TINS_SPLIT_BY_YEAR = [
    "L001-100044638", "L001-100103383", "L001-100063098", "L001-104015799",
    "L001-107032791", "L001-100050115", "L001-100181805", "L001-100045138",
    "L001-100057551", "L001-100045766", "L001-100055338", "L001-100011888",
    "L001-901705421", "L001-107000385", "L001-100045707", "L001-100136036",
    "L001-103007369", "L001-100046460", "L001-100044956", "L001-100099718",
    "L001-901503317", "L001-100191681", "L001-100121322", "L001-100121845",
    "L001-100053564", "L001-901501877", "L001-100129250", "L001-100045995",
    "L001-100066011", "L001-100048706", "L001-107041723", "L001-901705389",
    "L001-107030519", "L001-100112145", "L001-106004107", "L001-100190324",
    "L001-100186432", "L001-104013273", "L001-100045782", "L001-100098630",
    "L001-105001619", "L001-100045162", "L001-107040190", "L001-100068901",
    "L001-100074669", "L001-901639143", "L001-100049648", "L001-107024462",
    "L001-150000308", "L001-100120679", "L001-100049915", "L001-901501119"
]

SPECIAL_TINS_9_DIGITS = {t[-9:] for t in TINS_SPLIT_BY_YEAR}

class ProgressTracker:
    _instances = {}

    @classmethod
    def update(cls, task_id, progress, status, details="", failed_file=None):
        # Initialize the task if it doesn't exist
        if task_id not in cls._instances:
            cls._instances[task_id] = {
                'progress': 0,
                'status': 'Pending',
                'details': '',
                'failed_file': None,
                'logs': []
            }

        # Create a formatted terminal log entry
        time_str = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{time_str}] {status}: {details}"

        # Update state
        cls._instances[task_id]['progress'] = progress
        cls._instances[task_id]['status'] = status
        cls._instances[task_id]['details'] = details
        cls._instances[task_id]['failed_file'] = failed_file
        
        # Append to the terminal history (preventing identical back-to-back spam)
        if not cls._instances[task_id]['logs'] or cls._instances[task_id]['logs'][-1] != log_entry:
            cls._instances[task_id]['logs'].append(log_entry)

    @classmethod
    def get(cls, task_id):
        return cls._instances.get(task_id, {
            'progress': 0, 'status': 'Pending', 'details': '', 'failed_file': None, 'logs': []
        })

class ConsolidationService:
    def __init__(self, task_id, user):
        self.task_id = task_id
        self.user = user
        os.makedirs(APPDATA_DIR, exist_ok=True)
        self.con = duckdb.connect(DB_PATH)
        
        # --- DYNAMIC OUTPUT DIRECTORY (From Core) ---
        try:
            user_settings = UserSettings.objects.get(user=user)
            self.output_dir = user_settings.default_output_dir
        except UserSettings.DoesNotExist:
            if os.path.exists("D:/"):
                self.output_dir = "D:/AuditCore_Output"
            else:
                self.output_dir = os.path.join(os.path.expanduser("~"), "AuditCore_Output")
        
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

    def log(self, percent, status, details, failed_file=None):
        ProgressTracker.update(self.task_id, percent, status, details, failed_file)

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
        current_file = "Unknown"
        try:
            self.log(5, "Initializing", "Preparing files...")
            extracted_files = []

            # 1. EXTRACT
            for f_path in file_paths:
                f_name = os.path.basename(f_path)
                current_file = f_name
                if f_path.lower().endswith(('.rar', '.zip')):
                    self.log(10, "Extracting", f"Extracting {f_name}...")
                    extract_to = os.path.join(BASE_DIR, 'temp_extract', str(uuid.uuid4()))
                    os.makedirs(extract_to, exist_ok=True)
                    try:
                        patoolib.extract_archive(f_path, outdir=extract_to, verbosity=-1)
                        for root, _, files in os.walk(extract_to):
                            for file in files:
                                if file.lower().endswith(('.xlsx', '.xls')) and not file.startswith('~$'):
                                    extracted_files.append(os.path.join(root, file))
                    except Exception as e:
                         logger.error(f"Extraction failed: {e}")
                elif f_path.lower().endswith(('.xlsx', '.xls')):
                    extracted_files.append(f_path)

            # 2. IMPORT
            total = len(extracted_files)
            if total == 0:
                self.log(100, "Error", "No valid Excel files found.", failed_file=current_file)
                return

            self.log(20, "Importing", f"Found {total} files. Importing...")
            
            tins_processed = set()
            tin_company_map = {} 
            failed_files = []
            
            for i, file in enumerate(extracted_files):
                fname = os.path.basename(file)
                current_file = fname
                
                # Math Fix: Show before reading
                percent_before = 20 + int((i / total) * 30)
                self.log(percent_before, "Importing", f"Reading {fname} ({i+1} of {total})")
                
                # SMART TIN EXTRACTION
                match_full = re.search(r'([A-Za-z0-9]{4}-\d{9})', fname)
                if match_full:
                    tin = match_full.group(1).upper()
                else:
                    match_short = re.search(r'(?<!\d)(\d{9})(?!\d)', fname)
                    tin = match_short.group(1) if match_short else "UNKNOWN"
                
                tins_processed.add(tin)
                branch_info = ""
                
                try:
                    df = pd.read_excel(file, skiprows=3, dtype=str)
                    if len(df.columns) < 10:
                        raise ValueError("File structure does not match expected format.")
                        
                    title_df = pd.read_excel(file, nrows=1, header=None)
                    title_val = title_df.iloc[0, 0] if not title_df.empty else ""
                    
                    if title_val and isinstance(title_val, str):
                        if tin not in tin_company_map:
                            cleaned_name = self.clean_company_name(title_val)
                            tin_company_map[tin] = cleaned_name
                        branch_info = self.extract_branch_info(title_val)
                    
                    df.columns = [f'Column{j}' for j in range(len(df.columns))]
                    safe_branch = branch_info.replace("'", "''")
                    
                    self.con.register('df_view', df)

                    core_fields_str = """
                        date, invoice_number, credit_notification_letter_number, buyer_type,
                        tax_registration_id, buyer_name, total_invoice_amount, 
                        amount_exclude_vat, non_vat_sales, vat_zero_rate, vat_local_sale,
                        vat_export, vat_local_sale_state_burden, vat_withheld_by_national_treasury,
                        plt, special_tax_on_goods, special_tax_on_services, accommodation_tax,
                        income_tax_redemption_rate, notes, description, tax_declaration_status
                    """

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
                                COALESCE(TRY_STRPTIME(Column1, '%d-%m-%Y')::DATE, TRY_CAST(Column1 AS DATE)) as date,
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
                    
                    # Math Fix: Show completion of this file
                    percent_after = 20 + int(((i + 1) / total) * 30)
                    self.log(percent_after, "Importing", f"Ingested {fname}")
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Failed to import {fname}: {error_msg}")
                    failed_files.append(f"❌ {fname} (Reason: {error_msg})")
                    continue

            # 3. CONSOLIDATE & EXPORT
            self.log(50, "Consolidating", "Generating Master Files...")
            all_summary_stats = []
            
            total_tins = len(tins_processed)
            if total_tins == 0:
                self.log(100, "Failed", "No valid data could be extracted.", failed_file=current_file)
                return

            # Calculate how much % each TIN is worth (total phase is 40%)
            tin_weight = 40.0 / total_tins

            for idx, tin in enumerate(tins_processed):
                tin_base_pct = 50 + (idx * tin_weight)
                
                # Micro-Step 1: Querying Data (10% of this TIN's progress)
                self.log(int(tin_base_pct + (tin_weight * 0.1)), "Consolidating", f"Querying database for {tin} ({idx + 1}/{total_tins})...")
                
                df_all = self.con.sql(f"""
                    SELECT * FROM tax_declaration 
                    WHERE file_tin = '{tin}'
                    ORDER BY date ASC
                """).to_df()
                
                if df_all.empty:
                    self.log(int(tin_base_pct + tin_weight), "Consolidating", f"Skipped {tin} (No data)")
                    continue

                # Micro-Step 2: Stats Collection (20% of this TIN's progress)
                self.log(int(tin_base_pct + (tin_weight * 0.2)), "Consolidating", f"Analyzing statistics for {tin}...")
                
                df_all['year'] = df_all['date'].dt.year
                df_all['month'] = df_all['date'].dt.month
                
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

                # Micro-Step 3: Setup Excel (30% of this TIN's progress)
                self.log(int(tin_base_pct + (tin_weight * 0.3)), "Consolidating", f"Building Excel workbook for {tin}...")
                
                company_name = tin_company_map.get(tin, tin)
                min_year = int(df_all['year'].min())
                max_year = int(df_all['year'].max())
                total_years_span = max(1, max_year - min_year + 1)

                chunk_size = 1 if str(tin)[-9:] in SPECIAL_TINS_9_DIGITS else 4
                
                master_file = os.path.join(self.output_dir, f"{tin}.xlsx")
                workbook = xlsxwriter.Workbook(master_file, {'nan_inf_to_errors': True})
                
                current_start = min_year
                has_sheets = False
                
                # Micro-Step 4: Writing Sheets (Scales dynamically up to 90% of this TIN's progress)
                while current_start <= max_year:
                    # Calculate how far through the years we are to update the progress bar inside the loop
                    progress_ratio = (current_start - min_year) / total_years_span
                    chunk_pct = tin_base_pct + (tin_weight * 0.3) + (tin_weight * 0.6 * progress_ratio)
                    
                    current_end = current_start + chunk_size - 1
                    df_chunk = df_all[(df_all['year'] >= current_start) & (df_all['year'] <= current_end)]
                    
                    if not df_chunk.empty:
                        has_sheets = True
                        c_min_date = df_chunk['date'].min()
                        c_max_date = df_chunk['date'].max()
                        sheet_name = f"{c_min_date.strftime('%m-%Y')} - {c_max_date.strftime('%m-%Y')}"[:31]
                        
                        self.log(int(chunk_pct), "Consolidating", f"Writing sheet '{sheet_name}' for {tin}...")
                        
                        ws = workbook.add_worksheet(name=sheet_name)
                        self.write_sheet_data_fast(workbook, ws, df_chunk, company_name)
                    
                    current_start += chunk_size
                
                if has_sheets:
                    workbook.close()

                # Micro-Step 5: Finished (100% of this TIN's progress)
                self.log(int(tin_base_pct + tin_weight), "Consolidating", f"Saved {tin}.xlsx")

            # 4. GENERATE SUMMARY REPORT
            self.log(90, "Finalizing", "Updating Summary Report...")
            self.generate_summary_report_fast(all_summary_stats)

            shutil.rmtree(os.path.join(BASE_DIR, 'temp_extract'), ignore_errors=True)
            
            if failed_files:
                skipped_list = "\n".join(failed_files)
                warning_msg = f"Processed successfully, but skipped {len(failed_files)} file(s):\n\n{skipped_list}"
                self.log(100, "Completed with Warnings", warning_msg)
            else:
                self.log(100, "Completed", f"Saved to {self.output_dir}")
        
        except Exception as e:
            logger.error(traceback.format_exc())
            self.log(100, "Failed", str(e), failed_file=current_file)
        finally:
            self.con.close()

    def write_sheet_data_fast(self, workbook, ws, df, company_name):
        """High-speed vectorized formatting using XlsxWriter matching original design exactly."""
        
        # --- PREPARE FORMATS ---
        f_title = workbook.add_format({'font_name': 'Khmer OS Muol Light', 'font_size': 12, 'align': 'left', 'valign': 'vcenter'})
        f_header = workbook.add_format({'font_name': 'Khmer OS Siemreap', 'font_size': 11, 'bold': True, 'bg_color': '#B0DBBC', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        
        # Data formats (Alternating rows + Number/Date variations)
        base_even = {'font_name': 'Khmer OS Siemreap', 'font_size': 11, 'border': 1, 'border_color': '#DDDDDD', 'bg_color': '#F5F5F5', 'valign': 'vcenter'}
        base_odd = {'font_name': 'Khmer OS Siemreap', 'font_size': 11, 'border': 1, 'border_color': '#DDDDDD', 'valign': 'vcenter'}
        
        f_even = workbook.add_format(base_even)
        f_odd = workbook.add_format(base_odd)
        
        f_even_date = workbook.add_format({**base_even, 'num_format': 'DD-MM-YYYY', 'align': 'center'})
        f_odd_date = workbook.add_format({**base_odd, 'num_format': 'DD-MM-YYYY', 'align': 'center'})
        
        f_even_num = workbook.add_format({**base_even, 'num_format': '#,##0'})
        f_odd_num = workbook.add_format({**base_odd, 'num_format': '#,##0'})

        # --- 1. SET COLUMN WIDTHS ---
        widths = [
            8, 6, 14, 18, 15, 15, 18, 30, 20, 18, 18, 18, 18, 18, 18, 
            18, 18, 18, 18, 18, 15, 20, 25, 20, 25
        ]
        for idx, w in enumerate(widths):
            ws.set_column(idx, idx, w)

        # --- 2. TITLE (Row 0) ---
        title = f"បញ្ជីទិន្នានុប្បវត្តិលក់ របស់ {company_name}"
        ws.merge_range(0, 0, 0, 24, title, f_title)

        # --- 3. HEADERS (Rows 1 & 2) ---
        vertical_merges = [
            (0, "ឆ្នាំ"), (1, "ល.រ"), (2, "កាលបរិច្ឆេទ"), (3, "លេខវិក្កយបត្រ ឬប្រតិវេទគយ"), 
            (4, "លេខលិខិតជូនដំណឹងឥណទាន"), (8, "តម្លៃសរុបលើវិក្កយបត្រ"), (11, "អាករលើតម្លៃបន្ថែមអត្រា ០% "),
            (14, "អាករលើតម្លៃបន្ថែម(បន្ទុករដ្ឋ)"), (15, "អតប កាត់ទុកដោយរតនាគារជាតិ"), 
            (16, "អាករបំភ្លឺសាធារណៈ"), (17, "អាករពិសេសលើទំនិញមួយចំនួន"), 
            (18, "អាករពិសេសលើសេវាមួយចំនួន"), (19, "អាករលើការស្នាក់នៅ"), 
            (20, "អត្រាប្រាក់រំដោះពន្ធលើប្រាក់ចំណូល"), (21, "កំណត់សម្គាល់"), 
            (22, "បរិយាយ"), (23, "ស្ថានភាពប្រកាសពន្ធ"), (24, "ស្នាក់ការ")
        ]
        
        for col, text in vertical_merges:
            ws.merge_range(1, col, 2, col, text, f_header)

        # Horizontal group merges
        ws.merge_range(1, 5, 1, 7, "អ្នកទិញ", f_header)
        ws.write(2, 5, "ប្រភេទ", f_header)
        ws.write(2, 6, "លេខសម្គាល់ចុះបញ្ជីពន្ធដា", f_header)
        ws.write(2, 7, "ឈ្មោះ", f_header)

        ws.merge_range(1, 9, 1, 10, "តម្លៃ​​មិន​រួមអតប និងមិនជាប់​អតប​", f_header)
        ws.write(2, 9, "តម្លៃមិនរួមអតប", f_header)
        ws.write(2, 10, "ការលក់មិនជាប់អតប", f_header)

        ws.merge_range(1, 12, 1, 13, "អាករលើតម្លៃបន្ថែម", f_header)
        ws.write(2, 12, "ការលក់ក្នុងស្រុក", f_header)
        ws.write(2, 13, "អតបលើការនាំចេញ", f_header)

        # --- 4. DATA WRITING (High Speed Row Writes) ---
        df = df.sort_values('date')
        row_idx = 3
        counter = 1
        previous_year = None
        
        for _, row in df.iterrows():
            year = row['date'].year if pd.notnull(row['date']) else ""
            if previous_year is not None and year != previous_year:
                row_idx += 1 
            previous_year = year

            is_even = (counter % 2 == 0)
            fmt_base = f_even if is_even else f_odd
            fmt_date = f_even_date if is_even else f_odd_date
            fmt_num = f_even_num if is_even else f_odd_num

            # Array matching the 25 columns
            vals = [
                year, counter, row['date'], row['invoice_number'], row['credit_notification_letter_number'],
                row['buyer_type'], row['tax_registration_id'], row['buyer_name'], row['total_invoice_amount'],
                row['amount_exclude_vat'], row['non_vat_sales'], row['vat_zero_rate'], row['vat_local_sale'],
                row['vat_export'], row['vat_local_sale_state_burden'], row['vat_withheld_by_national_treasury'],
                row['plt'], row['special_tax_on_goods'], row['special_tax_on_services'], row['accommodation_tax'],
                row['income_tax_redemption_rate'], row['notes'], row['description'], row['tax_declaration_status'], 
                row['branch_name']
            ]
            
            for c_idx, val in enumerate(vals):
                if pd.isna(val) or val is None:
                    ws.write(row_idx, c_idx, "", fmt_base)
                elif c_idx == 2:
                    ws.write_datetime(row_idx, c_idx, val, fmt_date)
                elif 8 <= c_idx <= 19:
                    ws.write_number(row_idx, c_idx, float(val), fmt_num)
                else:
                    ws.write(row_idx, c_idx, str(val), fmt_base)

            row_idx += 1
            counter += 1

    def generate_summary_report_fast(self, summary_data_list):
        """High-speed vectorized summary report generator matching openpyxl design."""
        if not summary_data_list:
            return None

        try:
            filepath = os.path.join(self.output_dir, "0.Import_Summary.xlsx")
            
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
            
            df = pd.DataFrame(final_data)
            if df.empty: return None
            if 'No' in df.columns: df = df.drop(columns=['No'])

            df['TIN'] = df['TIN'].astype(str)
            df = df.sort_values(by=['TIN', 'Year'])
            
            total_transactions = df['Total Transactions'].sum()
            unique_tins_count = df['TIN'].nunique()

            # 3. WRITE VIA XLSXWRITER
            workbook = xlsxwriter.Workbook(filepath)
            worksheet = workbook.add_worksheet('Summary')
            
            # Configure columns
            worksheet.set_column(0, 0, 15)
            worksheet.set_column(1, 1, 25)
            worksheet.set_column(2, 2, 35)
            worksheet.set_column(3, 3, 10)
            worksheet.set_column(4, 4, 20)
            
            # Formatting definitions
            f_head = workbook.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#4472C4', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            
            fmt_dict = {
                'white': {
                    'center': workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'}),
                    'left': workbook.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True}),
                    'num': workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': '#,##0'})
                },
                'grey': {
                    'center': workbook.add_format({'border': 1, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter'}),
                    'left': workbook.add_format({'border': 1, 'bg_color': '#D9E1F2', 'align': 'left', 'valign': 'vcenter', 'text_wrap': True}),
                    'num': workbook.add_format({'border': 1, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'num_format': '#,##0'})
                }
            }
            
            f_grand_total = workbook.add_format({'bold': True, 'bg_color': '#ACB9CA', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            f_grand_total_num = workbook.add_format({'bold': True, 'bg_color': '#ACB9CA', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': '#,##0'})

            # Write Headers
            headers = ["No", "TIN", "Month", "Year", "Total Transactions"]
            for col_num, data in enumerate(headers):
                worksheet.write(0, col_num, data, f_head)

            # Write Data with Smart Merging
            row_idx = 1
            tin_counter = 1
            use_grey = False

            for tin, group in df.groupby('TIN', sort=False):
                span = len(group)
                end_row = row_idx + span - 1
                color_key = 'grey' if use_grey else 'white'
                f_c = fmt_dict[color_key]['center']
                f_l = fmt_dict[color_key]['left']
                f_n = fmt_dict[color_key]['num']

                if span > 1:
                    worksheet.merge_range(row_idx, 0, end_row, 0, tin_counter, f_c)
                    worksheet.merge_range(row_idx, 1, end_row, 1, tin, f_c)
                else:
                    worksheet.write(row_idx, 0, tin_counter, f_c)
                    worksheet.write(row_idx, 1, tin, f_c)

                for i, (_, row_data) in enumerate(group.iterrows()):
                    c_row = row_idx + i
                    worksheet.write(c_row, 2, row_data['Month'], f_l)
                    worksheet.write(c_row, 3, row_data['Year'], f_c)
                    worksheet.write(c_row, 4, row_data['Total Transactions'], f_n)

                row_idx += span
                tin_counter += 1
                use_grey = not use_grey

            # Write Grand Total Row
            worksheet.write(row_idx, 0, "GRAND TOTAL", f_grand_total)
            worksheet.write(row_idx, 1, f"Total TINs: {unique_tins_count}", f_grand_total)
            worksheet.write(row_idx, 2, "", f_grand_total)
            worksheet.write(row_idx, 3, "", f_grand_total)
            worksheet.write(row_idx, 4, total_transactions, f_grand_total_num)

            workbook.close()
            return filepath

        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return None

def run_task(task_id, file_paths, user):
    service = ConsolidationService(task_id, user)
    service.process(file_paths)