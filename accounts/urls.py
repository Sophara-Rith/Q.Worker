from django.urls import path
from . import views

app_name = 'accounts'

from django.urls import path
from . import views

app_name = 'accounts'

urlpatterns = [
    path('login/', views.login_view, name='login'),
    path('register/', views.register_view, name='register'),
    path('activate/', views.activation_view, name='activate'),
    path('logout/', views.logout_view, name='logout'),
    path('password-reset/', views.password_reset_view, name='password_reset'),
    path('api/change-password/', views.change_password_ajax, name='change_password'),
    path('deactivate/', views.deactivate_license, name='deactivate_license'),
]