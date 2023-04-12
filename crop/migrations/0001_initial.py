from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CropData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.PositiveSmallIntegerField(help_text='Year of the harvest', unique=True)),
                ('corn_yield', models.IntegerField(help_text='Corn grain yield in the United States (measured in 1000s of megatons)')),
                ('created_timestamp', models.DateTimeField(auto_now_add=True)),
                ('updated_timestamp', models.DateTimeField(auto_now_add=True)),
            ],
        ),
    ]