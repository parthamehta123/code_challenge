from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crop', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='cropdata',
            name='corn_yield',
            field=models.FloatField(help_text='Corn grain yield in the United States (measured in 1000s of megatons)'),
        ),
    ]