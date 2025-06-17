from rest_framework import serializers
from .models import PastEvals, UserSignature

class PastEvalsSerializer(serializers.ModelSerializer):
    class Meta:
        model = PastEvals
        fields = ["user", "gain_percentage", "loss_percentage", "position_length", "algos_used", "intercept_range", "clean_range", "intercept_needed", "results","run_date"]

class UserSignatureSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserSignature
        fields = ["user", "placeholder", "algorithms", "created_at"]
