# core/context_processors.py
from .models import UserSettings

def global_settings(request):
    if request.user.is_authenticated:
        settings, _ = UserSettings.objects.get_or_create(user=request.user)
        return {'user_settings': settings}
    return {}