from rest_framework import serializers
from .models import CropData


class CropDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = CropData
        fields = ['year', 'corn_yield']