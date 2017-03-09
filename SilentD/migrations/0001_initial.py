# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
from django.conf import settings
import SilentD.models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Data',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('date', models.DateTimeField(auto_now_add=True)),
                ('user', models.CharField(default=b' ', max_length=200)),
                ('description', models.CharField(max_length=200, blank=True)),
                ('file', models.FileField(upload_to=SilentD.models.generate_path, blank=True)),
                ('name', models.CharField(max_length=200, blank=True)),
                ('type', models.CharField(max_length=20, blank=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Profile',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('rank', models.CharField(default=b'Diagnostic', max_length=100, choices=[(b'Diagnostic', b'Diagnostic'), (b'Research', b'Research'), (b'Manager', b'Manager'), (b'Quality', b'Quality'), (b'Super', b'Super')])),
                ('cfia_access', models.BooleanField(default=False)),
                ('lab', models.CharField(blank=True, max_length=100, choices=[(1, b'St-Johns'), (2, b'Dartmouth'), (3, b'Charlottetown'), (4, b'St-Hyacinthe'), (5, b'Longeuil'), (6, b'Fallowfield'), (7, b'Carling'), (8, b'Greater Toronto Area'), (9, b'Winnipeg'), (10, b'Saskatoon'), (11, b'Calgary'), (12, b'Lethbridge'), (13, b'Burnaby'), (14, b'Sidney'), (15, b'Other')])),
                ('user', models.OneToOneField(to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name='Project',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('date', models.DateTimeField(auto_now_add=True)),
                ('user', models.CharField(default=b' ', max_length=200)),
                ('description', models.CharField(max_length=200, blank=True)),
                ('num_files', models.IntegerField(default=0)),
                ('organism', models.CharField(max_length=200, blank=True)),
                ('reference', models.CharField(max_length=200, blank=True)),
                ('type', models.CharField(max_length=20, blank=True)),
                ('amr_results', models.CharField(max_length=50, blank=True)),
                ('geneseekr_results', models.FileField(null=True, upload_to=b'', blank=True)),
                ('srst2_results', models.FileField(null=True, upload_to=b'', blank=True)),
                ('files', models.ManyToManyField(to='SilentD.Data')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
