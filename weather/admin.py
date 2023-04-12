from django.contrib import admin
from .models import WeatherData,Statistics

admin.site.register(WeatherData)
admin.site.register(Statistics)