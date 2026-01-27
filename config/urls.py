from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', lambda request: redirect('dashboard:index')),
    
    path('dashboard/', include('dashboard.urls')),
    path('accounts/', include('accounts.urls')),
    
    # This line connects the URL, but the file above defines the namespace
    path('consolidation/', include('consolidation.urls')), 

    path('core/', include('core.urls')),
]