from weather import views
from django.urls import path
from .models import WeatherData, Statistics
from .serializers import WeatherDataSerializer, StatisticsSerializer

urlpatterns = [
    path('weather/', views.WeatherDataList.as_view(queryset=WeatherData.objects.all(),
                                            serializer_class=WeatherDataSerializer),
         name='weatherdata-list'),
    path('weather/stats/', views.StatisticsList.as_view(queryset=Statistics.objects.all(),
                                                serializer_class=StatisticsSerializer),
         name='weatherstats-list'),

]