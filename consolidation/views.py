import os
import platform
import subprocess
import uuid
import threading
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required

# Use the new services architecture
from consolidation.services import ProgressTracker, run_task
from core.models import UserSettings

@login_required
def index(request):
    """Renders the Consolidation Dashboard UI (index.html)"""
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    
    context = {
        'default_output_dir': user_settings.default_output_dir or os.path.join(settings.BASE_DIR, 'output_reports')
    }
    return render(request, 'consolidation/index.html', context)

@login_required
def upload_files(request):
    """Handles file uploads and triggers the high-speed DuckDB engine asynchronously."""
    if request.method == 'POST':
        files = request.FILES.getlist('files')
            
        if not files:
            return JsonResponse({'status': 'failed', 'error': 'No files provided.'}, status=400)

        # Create temporary folder for uploaded files
        task_id = str(uuid.uuid4())
        temp_dir = os.path.join(settings.BASE_DIR, 'temp_uploads', task_id)
        os.makedirs(temp_dir, exist_ok=True)
            
        saved_paths = []
        try:
            # 1. Save uploaded files
            for f in files:
                path = os.path.join(temp_dir, f.name)
                with open(path, 'wb+') as destination:
                    for chunk in f.chunks():
                        destination.write(chunk)
                saved_paths.append(path)

            # 2. Trigger the high-speed DuckDB Engine in a Background Thread!
            # This prevents the browser from timing out on massive files
            thread = threading.Thread(
                target=run_task, 
                args=(task_id, saved_paths, request.user)
            )
            thread.daemon = True
            thread.start()

            # 3. Return the task_id immediately so the frontend UI can start polling the progress bar
            return JsonResponse({'task_id': task_id})

        except Exception as e:
            # Cleanup only if immediate saving fails
            for path in saved_paths:
                if os.path.exists(path):
                    os.remove(path)
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
            return JsonResponse({"status": "failed", "error": str(e)}, status=500)

    return JsonResponse({'error': 'POST required'}, status=400)

@login_required
def open_output_folder(request):
    """Opens the configured output directory in the OS default file explorer."""
    try:
        user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
        path = user_settings.default_output_dir or os.path.join(settings.BASE_DIR, 'output_reports')
        
        os.makedirs(path, exist_ok=True)

        system_platform = platform.system()
        if system_platform == "Windows":
            subprocess.Popen(['explorer', os.path.normpath(path)])
        elif system_platform == "Darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
            
        return JsonResponse({'status': 'opened', 'path': path})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def get_status(request, task_id):
    """Returns the live progress of the consolidation task for the frontend UI."""
    return JsonResponse(ProgressTracker.get(task_id))