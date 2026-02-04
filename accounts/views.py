import json
from django.shortcuts import render, redirect
from django.contrib.auth import login as auth_login, logout, update_session_auth_hash
from django.contrib import messages
from .hardware_id import get_hardware_id
from .license_validator import is_license_valid, generate_license_key, activate_license
from django.contrib.auth.forms import AuthenticationForm, PasswordResetForm
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_POST

def login_view(request):
    # 1. Check License First
    is_valid, _ = is_license_valid()
    if not is_valid:
        return redirect('accounts:activate')

    # 2. Handle Login Form
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            auth_login(request, form.get_user())
            
            # --- FIX: Clear old session context on fresh login ---
            # This ensures Processing/Result pages are empty until a new file is uploaded
            # or a history item is selected.
            if 'ovatr_code' in request.session:
                del request.session['ovatr_code']
            
            return redirect('dashboard:index')
        else:
            messages.error(request, "Invalid username or password.")
    
    # Pass HWID for display in footer
    context = {'hwid': get_hardware_id()}
    return render(request, 'accounts/login.html', context)

def activation_view(request):
    # Get HWID to show to the user
    hwid = get_hardware_id()
    
    # For demo purposes, we calculate the "Answer" key so you can copy-paste it
    # In production, you would NEVER send this to the template.
    demo_unlock_key = generate_license_key(hwid)

    if request.method == "POST":
        input_key = request.POST.get('license_key')
        if activate_license(input_key):
            messages.success(request, "License Activated Successfully!")
            return redirect('accounts:login')
        else:
            messages.error(request, "Invalid License Key.")

    context = {
        'hwid': hwid,
        'demo_key': demo_unlock_key # REMOVE THIS IN PRODUCTION
    }
    return render(request, 'accounts/activation.html', context)

def logout_view(request):
    # Standard logout flushes the session data automatically
    logout(request)
    return redirect('accounts:login')

def password_reset_view(request):
    if request.method == "POST":
        form = PasswordResetForm(request.POST)
        if form.is_valid():
            # In a real online app, this sends the email:
            # form.save(request=request) 
            
            # For this offline/demo version, we show a success message
            messages.success(request, "If an account exists, a reset link has been sent.")
            return redirect('accounts:login')
        else:
            messages.error(request, "Please enter a valid email address.")
            
    return render(request, 'accounts/password_reset.html')

@login_required
@require_POST
def change_password_ajax(request):
    try:
        data = json.loads(request.body)
        old_password = data.get('old')
        new_password = data.get('new')

        user = request.user
        if not user.check_password(old_password):
            return JsonResponse({'success': False, 'message': 'Incorrect current password.'}, status=400)

        if len(new_password) < 8:
            return JsonResponse({'success': False, 'message': 'New password must be at least 8 characters.'}, status=400)

        user.set_password(new_password)
        user.save()
        
        # Keep the user logged in after password change
        update_session_auth_hash(request, user)
        
        return JsonResponse({'success': True, 'message': 'Password successfully updated.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)