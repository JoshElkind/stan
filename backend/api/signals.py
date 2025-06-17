from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import User
from .models import UserSignature

@receiver(post_save, sender=User)
def create_user_signature(sender, instance, created, **kwargs):
    if created:
        placeholder = f"{instance.id}/"  # this will be used as their S3 prefix
        UserSignature.objects.create(user=instance, placeholder=placeholder)
