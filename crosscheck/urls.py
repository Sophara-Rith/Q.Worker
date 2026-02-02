from django.urls import path
from . import views

app_name = 'crosscheck'

urlpatterns = [
    path('new/', views.new_crosscheck, name='new'),
    path('processing/', views.processing_view, name='processing'),

    path('results/', views.results_view, name='results'),
    path('api/get-results-data/', views.get_results_data, name='get_results_data'),
    path('api/update-row/', views.update_result_row, name='update_result_row'),
    
    # NEW: API to fetch history
    path('api/get-row-history/', views.get_row_history, name='get_row_history'),

    path('api/upload-init/', views.upload_init, name='upload_init'),
    path('save-company-info/', views.save_company_info, name='save_company_info'),
    path('api/save-taxpaid/', views.save_taxpaid, name='save_taxpaid'),
    path('api/save-purchase/', views.save_purchase, name='save_purchase'),
    path('api/save-sale/', views.save_sale, name='save_sale'),
    path('api/save-reverse-charge/', views.save_reverse_charge, name='save_reverse_charge'),

    path('api/generate-annex3/', views.generate_annex_iii, name='generate_annex3'),

    path('api/get-stats/', views.get_crosscheck_stats, name='get_stats'),

    path('api/download-report/', views.download_report, name='download_report'),
]