from django.test import TestCase
import django
django.setup()
from crop.models import CropData
from rest_framework.test import APITestCase
from rest_framework import status
from rest_framework.test import APIClient
from django.urls import reverse


class CropDataTest(TestCase):
    def setUp(self):
        CropData.objects.create(year=2023, corn_yield=100.0)
        CropData.objects.create(year=2024, corn_yield=100.1)

    def test_default_object_return_value_for_crop_model(self):
        """year and crop yield"""
        year1 = CropData.objects.get(year=2023)
        year2 = CropData.objects.get(year=2024)
        self.assertEqual(year1.__str__(), '2023 : 100.0')
        self.assertEqual(year2.__str__(), '2024 : 100.1')


class CropDataListViewTest(APITestCase):

    def setUp(self):
        self.client = APIClient()

    def test_crop_data_list_api_view(self):
        url = reverse('cropdata-list')
        response = self.client.get(url, format='json')
        print(response.status_code)
        self.assertEqual(response.status_code, status.HTTP_200_OK)