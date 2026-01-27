from django.shortcuts import render, redirect
from accounts.license_validator import is_license_valid
from django.contrib.auth.decorators import login_required

def index(request):
    # --- SECURITY CHECK ---
    is_valid, _ = is_license_valid()
    if not is_valid:
        return redirect('accounts:activate')
    # ----------------------

    context = {
        'total_sessions': 24,
        'match_rate': 98.5,
        'active_tasks': 0
    }
    return render(request, 'dashboard/index.html', context)