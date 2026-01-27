from django.urls import path
from . import views

app_name = 'consolidation'

urlpatterns = [
    path('', views.index, name='index'),
    path('upload/', views.upload_files, name='upload'),
    path('status/<str:task_id>/', views.get_status, name='status'),
    path('open-folder/', views.open_output_folder, name='open_folder'),
]