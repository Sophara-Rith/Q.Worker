from django.urls import path
from . import views

app_name = 'crosscheck'

urlpatterns = [
    path('new/', views.new_crosscheck, name='new'),
    path('api/upload-init/', views.upload_init, name='upload_init'),
]