from django.db import models
from django.contrib.auth.models import User
import os

class UserSettings(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='settings')
    default_output_dir = models.CharField(
        max_length=500, 
        default=os.path.join(os.path.expanduser("~"), "QWorker_Output"),
        help_text="Absolute path to the directory where processed files will be saved."
    )

    def __str__(self):
        return f"Settings ({self.user.username})"

class Notification(models.Model):
    TYPE_CHOICES = (
        ('INFO', 'Info'),
        ('SUCCESS', 'Success'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
    )

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notifications')
    title = models.CharField(max_length=255)
    message = models.TextField()
    notification_type = models.CharField(max_length=20, choices=TYPE_CHOICES, default='INFO')
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.user.username} - {self.title}"