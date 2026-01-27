from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from .models import UserSettings
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