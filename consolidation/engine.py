import pandas as pd
import duckdb
import os
from django.conf import settings
from django.core.files import File
from core.models import Notification  # Import Notification

def run_consolidation_process(task_instance):
    """
    This function wraps your original logic.
    task_instance: The ConsolidationTask model object.
    """
    try:
        # 1. Get file path
        input_path = task_instance.input_file.path
        
        # 2. Initialize DuckDB
        con = duckdb.connect(database=':memory:')
        
        # --- YOUR ORIGINAL LOGIC GOES HERE ---
        # Example:
        # df = pd.read_excel(input_path)
        # con.register('source_data', df)
        # result_df = con.execute("SELECT * FROM source_data WHERE ...").df()
        
        # For now, let's just create a dummy "processed" version
        # (Replace this with your actual logic)
        df = pd.read_excel(input_path) if input_path.endswith('.xlsx') else pd.read_csv(input_path)
        df['Processed_By'] = 'Q.Worker Engine'
        df['Status'] = 'Validated'
        
        # 3. Save Output
        output_filename = f"processed_{os.path.basename(input_path)}"
        output_path = os.path.join(settings.MEDIA_ROOT, 'temp', output_filename)
        
        # Ensure temp dir exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to Excel
        df.to_excel(output_path, index=False)
        
        # 4. Update Django Model
        with open(output_path, 'rb') as f:
            task_instance.output_file.save(output_filename, File(f))
            
        task_instance.status = 'COMPLETED'
        task_instance.log_message = "Successfully processed with DuckDB."
        task_instance.save()
        
        # Cleanup temp file
        if os.path.exists(output_path):
            os.remove(output_path)
            
        # --- SUCCESS NOTIFICATION ---
        if task_instance.user:
            Notification.objects.create(
                user=task_instance.user,
                title="Consolidation Task Complete",
                message=f"Task {task_instance.id} processed successfully.",
                notification_type='SUCCESS'
            )
            
    except Exception as e:
        task_instance.status = 'FAILED'
        task_instance.log_message = str(e)
        task_instance.save()
        print(f"Error in engine: {e}")
        
        # --- FAILURE NOTIFICATION ---
        if task_instance.user:
            Notification.objects.create(
                user=task_instance.user,
                title="Consolidation Task Failed",
                message=f"Task {task_instance.id} failed: {str(e)}",
                notification_type='ERROR'
            )