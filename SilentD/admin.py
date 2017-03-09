from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import User, Profile, Project, Data

admin.site.unregister(User)

class UserProfileInline(admin.StackedInline):
    model = Profile

class UserProfileAdmin(UserAdmin):
    inlines = [ UserProfileInline, ]

# Register your models here.
admin.site.register(User, UserProfileAdmin)
admin.site.register(Data)
admin.site.register(Project)
