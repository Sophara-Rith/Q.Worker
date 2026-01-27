from django.db import models
from django.contrib.auth.models import User
import os

def user_directory_path(instance, filename):
    # Upload to MEDIA_ROOT/user_<id>/<filename>
    return 'uploads/user_{0}/{1}'.format(instance.user.id, filename)

class ConsolidationTask(models.Model):
    STATUS_CHOICES = (
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    )

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    input_file = models.FileField(upload_to=user_directory_path)
    output_file = models.FileField(upload_to='results/%Y/%m/%d/', null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    log_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def filename(self):
        return os.path.basename(self.input_file.name)

    def __str__(self):
        return f"{self.user.username} - {self.filename()}"