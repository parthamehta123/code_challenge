from rest_framework import serializers
from .models import WeatherData, Statistics


class WeatherDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = WeatherData
        fields = ['station_id', 'date', 'max_temp', 'min_temp', 'precipitation']


class StatisticsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Statistics
        fields = ['station_id', 'year', 'avg_max_temp', 'avg_min_temp', 'total_precipitation']