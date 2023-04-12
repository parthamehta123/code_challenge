from django.db import models
from bulk_update_or_create import BulkUpdateOrCreateQuerySet


class CropData(models.Model):
    """
        CropData model is used to store crop yield data in the United States by year
        """
    class Meta:
        ordering = ['year']

    year = models.PositiveSmallIntegerField(
        unique=True, help_text="Year of the harvest"
    )
    corn_yield = models.FloatField(
        help_text="Corn grain yield in the United States (measured in 1000s of megatons)"
    )
    created_timestamp = models.DateTimeField(auto_now_add=True, null=False)
    updated_timestamp = models.DateTimeField(auto_now_add=True, null=False)

    objects = BulkUpdateOrCreateQuerySet.as_manager()

    def __str__(self):
        return str(self.year) + " : " + str(self.corn_yield)