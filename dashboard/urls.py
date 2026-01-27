from django.urls import path
from . import views

# This sets the namespace to 'dashboard'
app_name = 'dashboard'

urlpatterns = [
    # This sets the pattern name to 'index'
    path('', views.index, name='index'), 
]