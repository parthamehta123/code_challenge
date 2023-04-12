from django.db import models
from bulk_update_or_create import BulkUpdateOrCreateQuerySet


class WeatherData(models.Model):
    """
    WeatherData model stores data collected daily from weather stations.
    """

    station_id = models.CharField(max_length=30, help_text="Weather station identifier")
    date = models.DateField(help_text="Date measured")
    max_temp = models.FloatField(
        help_text="The maximum temperature for that day (in tenths of a degree Celsius)",
    )
    min_temp = models.FloatField(
        help_text="The minimum temperature for that day (in tenths of a degree Celsius)",
    )
    precipitation = models.FloatField(
        help_text="The amount of precipitation for that day (in tenths of a millimeter)",
    )
    created_timestamp = models.DateTimeField(auto_now_add=True, null=False)
    updated_timestamp = models.DateTimeField(auto_now=True, null=False)
    objects = BulkUpdateOrCreateQuerySet.as_manager()

    class Meta:
        ordering = ['station_id']
        constraints = [
            models.UniqueConstraint(fields=['station_id', 'date'], name='unique station data for particular day')
        ]

    def __str__(self):
        return self.station_id + '-' + str(self.date) + ':' + str(self.min_temp) + ',' + str(self.max_temp) \
               + ',' + str(self.precipitation)


class Statistics(models.Model):
    """
    Statistics model stores basic stats calculated from subsets of WeatherData .
    """

    station_id = models.CharField(max_length=30, help_text="Weather station identifier")
    year = models.PositiveSmallIntegerField(
        help_text="statistics were calculated for this year"
    )
    avg_max_temp = models.FloatField(
        null=True,
        help_text="Average maximum temperature (in degrees Celsius)",
    )
    avg_min_temp = models.FloatField(
        null=True,
        help_text="Average minimum temperature (in degrees Celsius)",
    )
    total_precipitation = models.FloatField(
        null=True,
        help_text="Total accumulated precipitation (in centimeters",
    )

    created_timestamp = models.DateTimeField(auto_now_add=True, null=False)
    updated_timestamp = models.DateTimeField(auto_now=True, null=False)

    class Meta:
        ordering = ['station_id', 'year']
        constraints = [
            models.UniqueConstraint(fields=['station_id', 'year'], name='unique station data for particular year')
        ]

    def __str__(self):
        return self.station_id + '-' + str(self.year) + ':' + str(self.avg_min_temp) + ',' + str(self.avg_max_temp) \
               + ',' + str(self.total_precipitation)