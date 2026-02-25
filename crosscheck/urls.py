from django.urls import path
from . import views

app_name = 'crosscheck'

urlpatterns = [
    # --- Views ---
    path('new/', views.new_crosscheck, name='new'),
    path('processing/', views.processing_view, name='processing'),
    path('results/', views.results_view, name='results'),
    path('history/', views.history_view, name='history'),

    # --- REPORTING MODULE ROUTES (NEW) ---
    path('report/', views.report_view, name='report'),
    path('api/report/data/', views.get_report_data, name='get_report_data'),
    path('api/report/update/', views.update_report_cell, name='update_report_cell'),
    path('download_full_report/', views.download_full_report, name='download_full_report'),
    path('download_word_report/', views.download_word_report, name='download_word_report'),

    # --- APIs (Existing) ---
    path('api/get-history/', views.get_history_api, name='get_history_api'),
    path('api/get-results-data/', views.get_results_data, name='get_results_data'),
    path('api/update-row/', views.update_result_row, name='update_result_row'),
    path('api/get-row-history/', views.get_row_history, name='get_row_history'),
    path('api/upload-init/', views.upload_init, name='upload_init'),
    path('api/run-processing/', views.run_processing_engine, name='run_processing_engine'),
    
    # --- Save APIs (Existing) ---
    path('save-company-info/', views.save_company_info, name='save_company_info'),
    path('api/save-taxpaid/', views.save_taxpaid, name='save_taxpaid'),
    path('api/save-purchase/', views.save_purchase, name='save_purchase'),
    path('api/save-sale/', views.save_sale, name='save_sale'),
    path('api/save-reverse-charge/', views.save_reverse_charge, name='save_reverse_charge'),

    # --- Processing & Stats APIs (Existing) ---
    path('api/generate-annex3/', views.generate_annex_iii, name='generate_annex3'),
    path('api/get-stats/', views.get_crosscheck_stats, name='get_stats'),
    
    # --- Download APIs ---
    # NOTE: download_report is for the Annex III Result file (Results Module)
    path('api/download-report/', views.download_report, name='download_report'),

    # --- Crosscheck Status APIs ---
    path('api/user-statuses/', views.api_user_statuses, name='api_user_statuses'),
]