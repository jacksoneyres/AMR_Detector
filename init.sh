#!/usr/bin/env bash

echo "Linking Hashes"
ln -s /app/documents/Current_Hashes/* /app/

echo "Migrating Database If Needed"
cd /app/ && python manage.py makemigrations
cd /app/ && python manage.py migrate

echo "Setting Up Django"
cd /app/ && python manage.py runserver 0.0.0.0:8000 &
echo "Setting Up Celery"
# Starts Celery, Flower and workers given 24 hours max to complete a task and output logs
cd /app/ && python manage.py celery worker --time-limit=86400 --loglevel=info 
#echo "Setting Up Flower"
#cd /app/ && python manage.py celery flower
