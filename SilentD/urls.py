from django.conf.urls import patterns, url
from SilentD import views
from django.views.generic import TemplateView

urlpatterns = patterns(
    '',
    url(r'^register/$', views.register, name='register'),
    url(r'^logout/$', views.user_logout, name='logout'),
    url(r'^file_upload/$', views.file_upload, name='file_upload'),
    url(r'^index',  TemplateView.as_view(template_name='SilentD/index.html'), name='index'),
    url(r'^amr/$', views.amr, name='amr')
)
