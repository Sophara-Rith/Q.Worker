from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    path('settings/', views.settings_view, name='settings'),
    path('browse/', views.browse_directory, name='browse'),
    path('save-settings/', views.save_settings_ajax, name='save_settings'),
    path('get-settings/', views.get_settings_json, name='get_settings'),
    path('download-template/', views.download_excel_template, name='download_template'),

    path('api/system-stats/', views.get_system_stats, name='system_stats'),
]