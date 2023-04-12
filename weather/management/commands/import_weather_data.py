from django.core.management.base import BaseCommand
from weather.models import WeatherData
import pandas as pd
import os
import datetime


class Command(BaseCommand):
    help = 'import weather data'

    def add_argument(self, parser):
        pass

    def format_date(self, date):
        return date[:4] + '-' + date[4:6]+'-' + date[6:]

    def handle(self, *args, **options):
        num_of_records_before_insert = WeatherData.objects.all().count()
        start_time = datetime.datetime.now()

        # loop through the weather data files
        files = os.listdir('wx_data/')
        for file in files:
            if file.endswith(".txt"):
                # load data into pandas data frame
                df = pd.read_csv("wx_data/"+str(file), sep="\t", header=None,
                                 names=['date', 'max_temp', 'min_temp', 'precipitation'])
                df_records = df.to_dict('records')

                # load data from data frame to model instances
                model_instances = [WeatherData(
                    station_id=str(file.split('.')[0]),
                    date=self.format_date(str(record['date'])),
                    max_temp=record['max_temp'],
                    min_temp=record['min_temp'],
                    precipitation=record['precipitation']
                ) for record in df_records]

                # use django bulk_create to insert data into tables
                WeatherData.objects.bulk_create(model_instances, ignore_conflicts=True)

        finish_time = datetime.datetime.now()
        num_of_records_after_insert = WeatherData.objects.all().count()
        # using print as log statement for test purposes
        print("Data Ingested for weather data \n")
        print("Start Time {}".format(start_time) + "\n")
        print("Finish Time {}".format(finish_time) + "\n")
        print("Number of records ingested: {}".format(
            num_of_records_after_insert - num_of_records_before_insert) + "\n")