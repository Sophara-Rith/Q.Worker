import subprocess
import sys
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
import psutil
from .models import UserSettings
import json
import os

@login_required
def settings_view(request):
    user_settings, created = UserSettings.objects.get_or_create(user=request.user)

    if request.method == 'POST':
        new_dir = request.POST.get('output_dir')
        if new_dir:
            # Save the manually entered or browsed path
            user_settings.default_output_dir = new_dir
            user_settings.save()
            messages.success(request, "Output directory updated!")
        else:
            messages.error(request, "Path cannot be empty.")
    
    return render(request, 'core/settings.html', {'settings': user_settings})

@login_required
def save_settings_ajax(request):
    """
    API endpoint to save settings via JavaScript (AJAX)
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            new_dir = data.get('output_dir')
            
            if not new_dir:
                 return JsonResponse({'success': False, 'error': "Path cannot be empty."})

            # Validate path
            if not os.path.exists(new_dir):
                try:
                    os.makedirs(new_dir, exist_ok=True)
                except OSError:
                     return JsonResponse({'success': False, 'error': "Invalid path or permission denied."})

            # Save
            settings, _ = UserSettings.objects.get_or_create(user=request.user)
            settings.default_output_dir = new_dir
            settings.save()
            
            return JsonResponse({'success': True})
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})

    return JsonResponse({'error': 'POST required'}, status=400)

@login_required
def get_settings_json(request):
    """API to fetch settings for the modal"""
    settings, _ = UserSettings.objects.get_or_create(user=request.user)
    return JsonResponse({'output_dir': settings.default_output_dir})

@login_required
def browse_directory(request):
    """
    Opens a native OS folder picker on the server (local machine).
    Uses a subprocess to avoid Tkinter thread issues in Django.
    """
    try:
        # One-liner Python script to open dialog and print result
        script = "import tkinter as tk, tkinter.filedialog as fd; root=tk.Tk(); root.withdraw(); print(fd.askdirectory())"
        
        # Run independent process
        result = subprocess.check_output([sys.executable, "-c", script], stderr=subprocess.STDOUT)
        
        # Decode path
        path = result.decode().strip()
        
        if path:
            return JsonResponse({'path': path})
        else:
            return JsonResponse({'status': 'cancelled'})
            
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def get_system_stats(request):
    """
    Returns system statistics for the live monitor.
    """
    # 1. CPU Usage (interval=0.1 ensures we get a fresh reading without blocking too long)
    cpu_usage = psutil.cpu_percent(interval=0.1)
    
    # 2. RAM Usage
    memory = psutil.virtual_memory()
    ram_usage = memory.percent
    ram_total_gb = round(memory.total / (1024**3), 1)
    ram_used_gb = round(memory.used / (1024**3), 1)
    
    # 3. Processing Speed (CPU Frequency)
    # Note: cpu_freq() can be None on some systems (e.g. some M1 Macs or restricted environments)
    freq = psutil.cpu_freq()
    if freq:
        speed_val = round(freq.current / 1000, 2) # Convert MHz to GHz
        speed_text = f"{speed_val} GHz"
    else:
        # Fallback if frequency is unavailable
        speed_text = "N/A"

    return JsonResponse({
        'cpu': cpu_usage,
        'ram_percent': ram_usage,
        'ram_text': f"{ram_used_gb}/{ram_total_gb} GB",
        'speed': speed_text
    })