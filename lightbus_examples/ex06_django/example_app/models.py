from django.db import models


class PageView(models.Model):
    url = models.CharField(max_length=255)
    viewed_at = models.DateTimeField(auto_now_add=True)
    user_agent = models.TextField()
