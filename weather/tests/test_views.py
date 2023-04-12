from django.test import TestCase
import django
django.setup()
from weather.models import WeatherData, Statistics
import datetime
from rest_framework.test import APITestCase
from rest_framework import status
from rest_framework.test import APIClient
from django.urls import reverse


class WeatherDataTest(TestCase):
    def setUp(self):
        WeatherData.objects.create(station_id='1234', date=datetime.datetime.now().date(), min_temp=34.0,
                                   max_temp=50.0, precipitation=22)

    def test_default_object_return_value(self):
        """year and crop yield"""
        todays_date = str(datetime.datetime.now().date())
        station = WeatherData.objects.get(station_id='1234')
        self.assertEqual(station.__str__(), '1234-' + todays_date + ':34.0,50.0,22.0')


class WeatherDataListTest(APITestCase):

    def setUp(self):
        self.client = APIClient()

    def test_weather_data_list_api_view(self):
        url = reverse('weatherdata-list')
        response = self.client.get(url, format='json')
        print(response.status_code)
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class StatisticsDataTest(TestCase):
    def setUp(self):
        Statistics.objects.create(station_id='1234', year=datetime.datetime.now().date().year, avg_min_temp=34.0,
                                  avg_max_temp=50.0, total_precipitation=22)

    def test_default_object_return_value(self):
        """year and crop yield"""
        todays_date_year = str(datetime.datetime.now().date().year)
        station = Statistics.objects.get(station_id='1234')
        self.assertEqual(station.__str__(), '1234-' + str(todays_date_year) + ':34.0,50.0,22.0')


class StatisticsDataListTest(APITestCase):

    def setUp(self):
        self.client = APIClient()

    def test_weather_data_list_api_view(self):
        url = reverse('weatherstats-list')
        response = self.client.get(url, format='json')
        print(response.status_code)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
