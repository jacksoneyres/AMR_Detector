#!/usr/bin/env bash

echo "Migrating Database If Needed"
cd /app/ && python manage.py makemigrations
cd /app/ && python manage.py migrate

echo "Setting Up Django"
echo "from django.contrib.auth.models import User; User.objects.filter(email='admin@example.com').delete(); User.objects.create_superuser('admin', 'admin@example.com', 'admin')" | python manage.py shell
cd /app/ && python manage.py runserver 0.0.0.0:8000 &
echo "Setting Up Celery"
# Starts Celery and workers given 24 hours max to complete a task and output logs
cd /app/ && python manage.py celery worker --time-limit=86400 --loglevel=info 

