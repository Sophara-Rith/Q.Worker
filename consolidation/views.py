import os
import uuid
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages

from consolidation.services import ProgressTracker, run_task
from .models import ConsolidationTask
from .engine import run_consolidation_process
import threading

@login_required
def index(request):
    return render(request, 'consolidation/index.html')

@login_required
def upload_files(request):
    if request.method == 'POST':
        files = request.FILES.getlist('files') # 2. Supports Multiple Files
        
        if not files:
            return JsonResponse({'error': 'No files'}, status=400)

        # Save temp files
        task_id = str(uuid.uuid4())
        temp_dir = os.path.join(settings.BASE_DIR, 'temp_uploads', task_id)
        os.makedirs(temp_dir, exist_ok=True)
        
        saved_paths = []
        for f in files:
            path = os.path.join(temp_dir, f.name)
            with open(path, 'wb+') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)
            saved_paths.append(path)

        # Start Background Thread
        thread = threading.Thread(
            target=run_task, 
            args=(task_id, saved_paths, request.user)
        )
        thread.daemon = True
        thread.start()

        return JsonResponse({'task_id': task_id})

    return JsonResponse({'error': 'POST required'}, status=400)

@login_required
def get_status(request, task_id):
    return JsonResponse(ProgressTracker.get(task_id))

@login_required
def dashboard_view(request):
    """Main Dashboard showing history and upload form"""
    tasks = ConsolidationTask.objects.filter(user=request.user).order_by('-created_at')
    
    if request.method == 'POST' and request.FILES.get('file_upload'):
        uploaded_file = request.FILES['file_upload']
        
        # Create Task
        task = ConsolidationTask.objects.create(
            user=request.user,
            input_file=uploaded_file,
            status='PROCESSING'
        )
        
        # Run Engine in Background Thread (so browser doesn't freeze)
        thread = threading.Thread(target=run_consolidation_process, args=(task,))
        thread.start()
        
        messages.success(request, "File uploaded! Processing started...")
        return redirect('consolidation:dashboard')
        
    return render(request, 'consolidation/dashboard.html', {'tasks': tasks})