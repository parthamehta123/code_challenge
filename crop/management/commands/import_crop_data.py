from django.core.management.base import BaseCommand
from crop.models import CropData
import pandas as pd
import datetime


class Command(BaseCommand):
    help = 'import crop data'

    def add_argument(self, parser):
        pass

    def handle(self, *args, **options):
        num_of_records_before_insert = CropData.objects.all().count()
        start_time = datetime.datetime.now()
        # load data into pandas data frame
        df = pd.read_csv("yld_data/US_corn_grain_yield.txt", sep="\t", header=None, names=['year', 'corn_yield'])
        df_records = df.to_dict('records')

        # load data from data frame to model instances
        model_instances = [CropData(
            year=record['year'],
            corn_yield=record['corn_yield'],
        ) for record in df_records]

        # use django bulk_create to insert data into tables
        CropData.objects.bulk_create(model_instances, ignore_conflicts=True)
        finish_time = datetime.datetime.now()
        num_of_records_after_insert = CropData.objects.all().count()

        # using print as log statement for test purposes
        print("Data Ingested for crop data \n")
        print("Start Time {}".format(start_time) + "\n")
        print("Finish Time {}".format(finish_time) + "\n")
        print("Number of records ingested: {}".format(num_of_records_after_insert-num_of_records_before_insert) + "\n")