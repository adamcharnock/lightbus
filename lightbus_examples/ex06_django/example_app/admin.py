from django.contrib import admin

from .models import PageView


@admin.register(PageView)
class PageViewAdmin(admin.ModelAdmin):
    list_display = ["pk", "url", "viewed_at", "user_agent"]
    search_fields = ["pk", "url", "user_agent"]
    list_filter = ["viewed_at", "url"]
