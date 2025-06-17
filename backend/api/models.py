from django.contrib.auth.models import User
from django.db import models
from django.contrib.postgres.fields import ArrayField

class PastEvals(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    gain_percentage = models.FloatField()
    loss_percentage = models.FloatField()
    position_length = models.IntegerField()
    algos_used = models.JSONField(default=list, blank=True)
    intercept_range = models.IntegerField()
    clean_range = models.IntegerField()
    intercept_needed = models.IntegerField()
    results = models.JSONField(default=list)
    run_date = models.DateField(auto_now_add=True)

    def __str__(self):
        return self.user.email
    
class UserSignature(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    placeholder = models.CharField(max_length=100, default="")  # e.g., "42/"
    algorithms = models.JSONField(default=list)  # Structure: [{"algoname": ..., "summary": ...}]
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.username} S3 Signature"