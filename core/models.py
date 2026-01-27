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