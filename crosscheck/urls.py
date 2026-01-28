from django.urls import path
from . import views

app_name = 'crosscheck'

urlpatterns = [
    path('new/', views.new_crosscheck, name='new'),
    path('api/upload-init/', views.upload_init, name='upload_init'),
    path('save-company-info/', views.save_company_info, name='save_company_info'),
    path('api/save-taxpaid/', views.save_taxpaid, name='save_taxpaid'),
    path('api/save-purchase/', views.save_purchase, name='save_purchase'),
    path('api/save-reverse-charge/', views.save_reverse_charge, name='save_reverse_charge'),

]