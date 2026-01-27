from django.urls import path
from . import views

app_name = 'accounts'

urlpatterns = [
    path('login/', views.login_view, name='login'),
    path('activate/', views.activation_view, name='activate'),
    path('logout/', views.logout_view, name='logout'),
    path('password-reset/', views.password_reset_view, name='password_reset'),
]