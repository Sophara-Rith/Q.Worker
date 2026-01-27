from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from .models import Notification, UserSettings
import json
import os

@login_required
def settings_view(request):
    # Get or Create Settings for the user
    user_settings, created = UserSettings.objects.get_or_create(user=request.user)

    if request.method == 'POST':
        new_dir = request.POST.get('output_dir')
        
        if new_dir:
            # Normalize path (handle slashes)
            clean_path = os.path.normpath(new_dir)
            
            # Basic validation: Check if drive/root exists
            root = os.path.splitdrive(clean_path)[0]
            if root and not os.path.exists(root):
                 messages.error(request, f"The drive {root} does not exist.")
            else:
                try:
                    # Try creating/validating permissions
                    os.makedirs(clean_path, exist_ok=True)
                    user_settings.default_output_dir = clean_path
                    user_settings.save()
                    messages.success(request, "Global output directory updated successfully!")
                except Exception as e:
                    messages.error(request, f"Error saving path: {e}")
        else:
            messages.error(request, "Path cannot be empty.")

    return render(request, 'core/settings.html', {'settings': user_settings})

@login_required
def get_notifications(request):
    """API to fetch unread notifications"""
    unread_count = Notification.objects.filter(user=request.user, is_read=False).count()
    # Fetch latest 10 notifications
    notifs = Notification.objects.filter(user=request.user).order_by('-created_at')[:10]
    
    data = []
    for n in notifs:
        data.append({
            'id': n.id,
            'title': n.title,
            'message': n.message,
            'type': n.notification_type,
            'is_read': n.is_read,
            'time': n.created_at.strftime("%H:%M"),
            'date': n.created_at.strftime("%Y-%m-%d"),
        })
    
    return JsonResponse({
        'unread_count': unread_count,
        'notifications': data
    })

@login_required
@require_POST
def clear_all_notifications(request):
    """Permanently delete ALL notifications for the user"""
    Notification.objects.filter(user=request.user).delete()
    return JsonResponse({'status': 'success'})

@login_required
@require_POST
def mark_notification_read(request, notification_id):
    """Mark single notification as read"""
    try:
        notif = Notification.objects.get(id=notification_id, user=request.user)
        notif.is_read = True
        notif.save()
        return JsonResponse({'status': 'success'})
    except Notification.DoesNotExist:
        return JsonResponse({'status': 'error'}, status=404)

@login_required
@require_POST
def mark_all_read(request):
    """Mark ALL notifications as read"""
    Notification.objects.filter(user=request.user, is_read=False).update(is_read=True)
    return JsonResponse({'status': 'success'})