from .models import CropData
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import generics
from .serializers import CropDataSerializer


class CropDataList(generics.ListAPIView):

    # crop_data = CropData.objects.all()
    # serializer = CropDataSerializer(crop_data, many=True)
    # return JsonResponse(serializer.data, safe=False)

    queryset = CropData.objects.all()
    serializer_class = CropDataSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['year']