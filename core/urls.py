from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    path('settings/', views.settings_view, name='settings'),

    path('api/notifications/', views.get_notifications, name='api_notifications'),
    path('api/notifications/<int:notification_id>/read/', views.mark_notification_read, name='mark_notification_read'),
    path('api/notifications/read-all/', views.mark_all_read, name='mark_all_read'),

    path('api/notifications/clear-all/', views.clear_all_notifications, name='clear_all_notifications'),
]