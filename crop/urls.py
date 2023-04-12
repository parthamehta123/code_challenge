from crop import views
from django.urls import path
from .models import CropData
from .serializers import CropDataSerializer

urlpatterns = [
    path('yield/', views.CropDataList.as_view(queryset=CropData.objects.all(),
                                              serializer_class=CropDataSerializer), name='cropdata-list'),
]