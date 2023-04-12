from .models import WeatherData, Statistics
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import generics
from .serializers import WeatherDataSerializer, StatisticsSerializer


class WeatherDataList(generics.ListAPIView):

    queryset = WeatherData.objects.all()
    serializer_class = WeatherDataSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['station_id', 'date']


class StatisticsList(generics.ListAPIView):

    queryset = Statistics.objects.all()
    serializer_class = StatisticsSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['station_id', 'year']