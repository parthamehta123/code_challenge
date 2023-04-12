from django.core.management.base import BaseCommand
from weather.models import WeatherData, Statistics
from django.db.models import Max, Min, Avg, Sum


class Command(BaseCommand):
    help = 'Analyze and insert/update Weather Statistics Data'

    def add_argument(self, parser):
        pass

    def format_date(self, date):
        return date[:4] + '-' + date[4:6]+'-' + date[6:]

    def handle(self, *args, **options):

        weather_data = WeatherData.objects.all().count()
        if weather_data == 0:
            print("No Data available to analyze weather")

        start_year = WeatherData.objects.aggregate(Min("date"))['date__min'].year
        end_year = WeatherData.objects.aggregate(Max("date"))['date__max'].year

        all_stations = set(WeatherData.objects.values_list("station_id"))
        for station in all_stations:
            temp_start_year = start_year
            while temp_start_year <= end_year:

                avg_max_temp = WeatherData.objects.exclude(max_temp=-9999).filter(station_id=station[0],
                                                                                  date__year=temp_start_year) \
                    .aggregate(Avg("max_temp"))['max_temp__avg']

                avg_min_temp = WeatherData.objects.exclude(max_temp=-9999).filter(station_id=station[0],
                                                                                  date__year=temp_start_year) \
                    .aggregate(Avg("min_temp"))['min_temp__avg']
                total_precipitation = WeatherData.objects.exclude(precipitation=-9999).filter(station_id=station[0],
                                                                                              date__year=temp_start_year) \
                    .aggregate(Sum("precipitation"))['precipitation__sum']

                # Updating or inserting weather statistics data into Statistics table
                Statistics.objects.update_or_create(station_id=station[0], year=temp_start_year,
                                                    avg_max_temp=avg_max_temp, avg_min_temp=avg_min_temp,
                                                    total_precipitation=total_precipitation)
                print('Weather statistics for Station_id-{}: year{} are inserted into the database'
                      .format(station[0], temp_start_year))
                temp_start_year += 1