from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Statistics',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('station_id', models.CharField(help_text='Weather station identifier', max_length=30)),
                ('year', models.PositiveSmallIntegerField(help_text='statistics were calculated for this year')),
                ('avg_max_temp', models.FloatField(help_text='Average maximum temperature (in degrees Celsius)', null=True)),
                ('avg_min_temp', models.FloatField(help_text='Average minimum temperature (in degrees Celsius)', null=True)),
                ('total_precipitation', models.FloatField(help_text='Total accumulated precipitation (in centimeters', null=True)),
                ('created_timestamp', models.DateTimeField(auto_now_add=True)),
                ('updated_timestamp', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name='WeatherData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('station_id', models.CharField(help_text='Weather station identifier', max_length=30)),
                ('date', models.DateField(help_text='Date measured')),
                ('max_temp', models.FloatField(help_text='The maximum temperature for that day (in tenths of a degree Celsius)')),
                ('min_temp', models.FloatField(help_text='The minimum temperature for that day (in tenths of a degree Celsius)')),
                ('precipitation', models.FloatField(help_text='The amount of precipitation for that day (in tenths of a millimeter)')),
                ('created_timestamp', models.DateTimeField(auto_now_add=True)),
                ('updated_timestamp', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.AddConstraint(
            model_name='weatherdata',
            constraint=models.UniqueConstraint(fields=('station_id', 'date'), name='unique station data for particular day'),
        ),
        migrations.AddConstraint(
            model_name='statistics',
            constraint=models.UniqueConstraint(fields=('station_id', 'year'), name='unique station data for particular year'),
        ),
    ]