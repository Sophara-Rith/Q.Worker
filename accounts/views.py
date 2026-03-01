import json
from django.shortcuts import render, redirect
from django.contrib.auth import login as auth_login, logout, update_session_auth_hash
from django.contrib.auth.models import User
from django.contrib import messages
from .hardware_id import get_hardware_id
from .license_validator import (
    is_license_valid, 
    generate_license_key, 
    activate_license, 
    deactivate_license,
    check_firebase_status
)
from django.contrib.auth.forms import AuthenticationForm, PasswordResetForm
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_POST

def login_view(request):
    current_hwid = get_hardware_id()
    
    # 1. Check Local License status
    local_is_valid, _ = is_license_valid()
    
    # 2. Check Firebase status (Takes max 3 seconds)
    firebase_status = check_firebase_status(current_hwid)
    
    # 3. DECISION MATRIX
    if firebase_status is False:
        # EXPLICITLY REVOKED BY ADMIN! Destroy local key so they can't bypass it.
        deactivate_license()
        messages.error(request, "Your license has been disabled by the Administrator.")
        return redirect('accounts:activate')
        
    elif firebase_status is True:
        # APPROVED BY ADMIN! Generate local key if they don't have it yet.
        if not local_is_valid:
            new_key = generate_license_key(current_hwid)
            activate_license(new_key)
            messages.success(request, "License auto-activated via secure server!")
            
    elif firebase_status is None:
        # OFFLINE. Rely entirely on the local key.
        if not local_is_valid:
            return redirect('accounts:activate')

    # 4. Handle Login Form (Only reachable if license is currently valid)
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            auth_login(request, form.get_user())
            
            # --- NEW: Remember Me Logic ---
            remember_me = request.POST.get('remember_me')
            if remember_me:
                # Keep them logged in for 7 days (604,800 seconds), overriding browser-close
                request.session.set_expiry(604800) 
            else:
                # Expire the moment they close the browser (0 seconds)
                request.session.set_expiry(0)
            # ------------------------------
            
            if 'ovatr_code' in request.session:
                del request.session['ovatr_code']
            
            return redirect('dashboard:index')
        else:
            messages.error(request, "Invalid username or password.")
    
    context = {'hwid': current_hwid}
    return render(request, 'accounts/login.html', context)

def activation_view(request):
    # Just display the HWID. 
    # The login_view handles the actual Firebase auto-activation now!
    hwid = get_hardware_id()
    
    context = {
        'hwid': hwid
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
            messages.error(request, "Please enter a valid username.")
            
    return render(request, 'accounts/password_reset.html')

def register_view(request):
    # 1. Check License First (Only licensed PCs can register accounts!)
    current_hwid = get_hardware_id()
    local_is_valid, _ = is_license_valid()
    
    if not local_is_valid:
        firebase_status = check_firebase_status(current_hwid)
        if firebase_status is not True:
            return redirect('accounts:activate')

    # 2. Handle Registration Form
    if request.method == "POST":
        fullname = request.POST.get('fullname', '').strip().upper()
        username = request.POST.get('username', '').strip().upper()
        pass1 = request.POST.get('password', '')
        pass2 = request.POST.get('password_confirm', '')

        # Make sure fullname is checked
        if not fullname or not username or not pass1 or not pass2:
            messages.error(request, "All fields are required.")
        elif pass1 != pass2:
            messages.error(request, "Passwords do not match.")
        elif len(pass1) < 8:
            messages.error(request, "Password must be at least 8 characters long.")
        elif User.objects.filter(username__iexact=username).exists():
            messages.error(request, "This username is already taken.")
        else:
            # Create the user
            user = User.objects.create_user(username=username, password=pass1)
            
            # --- NEW: Save the Full Name into the first_name field ---
            user.first_name = fullname
            user.save()
            # ---------------------------------------------------------
            
            auth_login(request, user)
            
            # Clear any residual session data
            if 'ovatr_code' in request.session:
                del request.session['ovatr_code']
                
            messages.success(request, f"Welcome, {fullname}! Account created successfully.")
            return redirect('dashboard:index')

    return render(request, 'accounts/register.html')

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