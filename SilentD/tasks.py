# Python Library
from __future__ import absolute_import
from celery import Celery
from subprocess import call, check_output
import csv
import json
import zipfile
import shutil
import os
import glob
# Django Model Imports
from SilentD.models import Project


# Development Boolean
DEV = True
DOCKER_REGISTRY = "192.168.1.5:5000"
# General Settings
app = Celery('tasks',
             backend='djcelery.backends.database.DatabaseBackend',
             broker='amqp://guest:guest@localhost:5672//')


NAS_MOUNT_VOLUME = "/home/ubuntu/nas0/Genomics_Portal/documents_test/"


@app.task(bind=True)
def amr_task(self, obj_id):
    print self
    project_obj = Project.objects.get(id=obj_id)
    data_files = project_obj.files.all()

    for data in data_files:
        path = data.file.name.split('/')[-1]
        working_dir = 'documents/AMR/%s/%s' % (project_obj.user, project_obj.id)
        end_path = os.path.join(working_dir, path)

        print "Copying from %s to %s" % (data.file.name, end_path)
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)
        shutil.copyfile(data.file.name, end_path)

    if project_obj.type == "fasta":
            try:
                project_obj.amr_results = "Running"
                project_obj.save()
                distances = check_output(['./mash', 'dist', data.file.name, "documents/Mash/RefSeqMarch2017.msh"]).splitlines()
                distances_split = []
                for line in distances:
                    distances_split.append(line.split("\t"))
                if len(distances_split) > 0:
                    sorted_list = sorted(distances_split, key=lambda x: x[2])
                    project_obj.reference = sorted_list[0][1]
                    if "Escherichia_coli" in sorted_list[0][1]:
                        project_obj.organism = "Escherichia"
                    elif "Listeria" in sorted_list[0][1]:
                        project_obj.organism = "Listeria"
                    elif "Salmonella" in sorted_list[0][1]:
                        project_obj.organism = "Salmonella"
                    else: project_obj.organism = "Other"
                    project_obj.save()

                print distances_split[0]
                print sorted_list[0]
                print "Running GeneSeekR"

                call(['GeneSeekr', '-i', working_dir, '-o', working_dir, '-m', "documents/AMR/NCBI_AMR_170113.fasta", "-c", "90"])

                # Save Results, Check file exists, if so, make sure it actually contains results
                result_file = glob.glob(os.path.join(working_dir, '*.csv'))
                if len(result_file) == 1:
                    print "GeneSeekR File Detected"
                    project_obj.geneseekr_results.name = result_file[0]
                    project_obj.amr_results = "Success"
                    project_obj.save()

            except Exception as e:
                print "Error, GeneSeekR failed!", e.__doc__, e.message
                project_obj.amr_results = "Error"

    else:
        try:
            distances = check_output(['./mash', 'dist', data_files[0].file.name, "documents/Mash/RefSeqMarch2017.msh"]).splitlines()
            distances_split = []
            for line in distances:
                distances_split.append(line.split("\t"))
            if len(distances_split) > 0:
                sorted_list = sorted(distances_split, key=lambda x: x[2])
                project_obj.reference = sorted_list[0][1]
                if "Escherichia_coli" in sorted_list[0][1]:
                    print "detected"
                    project_obj.organism = "Escherichia"
                elif "Listeria" in sorted_list[0][1]:
                    project_obj.organism = "Listeria"
                elif "Salmonella" in sorted_list[0][1]:
                    project_obj.organism = "Salmonella"
                else:
                    project_obj.organism = "Other"
                project_obj.save()

                print distances_split[0]
                print sorted_list[0]
                print "Running SRST2"

                call(['srst2', '--input_pe', data_files[0].file.name, data_files[1].file.name, '--output', os.path.join(working_dir, str(project_obj.id)), '--gene_db', "documents/AMR/NCBI_AMR_170113_SRST2.fasta"])

                # Save Results, Check file exists, if so, make sure it actually contains results
                result_file = glob.glob(os.path.join(working_dir, '*fullgenes*.txt'))
                if len(result_file) == 1:
                    print "SRST2 File Detected"
                    project_obj.srst2_results.name = result_file[0]
                    project_obj.amr_results = "Success"
                    project_obj.save()

        except Exception as e:
            print "Error, SRST2 failed!", e.__doc__, e.message
            project_obj.amr_results = "Error"


@app.task(bind=True)
def amr_fastq_task(self, obj_id):
    print self

    # Retrieve objects from database
    project_obj = Project.objects.get(id=obj_id)
    data_files = project_obj.files.all()

    # Create the AMR database object
    amr_obj = AMR(user=project_obj.user, tag=project_obj.description, organism=project_obj.organism,
                  type=project_obj.type, job_id='1', project=project_obj)
    amr_obj.save()

    # Copy FASTQ Files to Working Directory
    data_path = 'documents/AMR/%s/%s' % (amr_obj.user, amr_obj.id)

    print "Copying FastQ Files Over to AMR Working Dir"
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    for f in data_files:
        path = f.file.name.split('/')[-1]
        end_path = data_path + '/' + path
        print "Copying from %s to %s" % (f.file.name, end_path)
        shutil.copyfile(f.file.name, end_path)

    print "Copying AMR Databases Over to AMR Working Dir"
    path_arg = 'documents/Targets/ARGannot.r1.fasta'
    path_res = 'documents/Targets/ResFinder.fasta'
    end_arg = os.path.join(data_path, 'ARGannot.fasta')
    end_res = os.path.join(data_path, 'ResFinder.fasta')

    shutil.copyfile(path_arg, end_arg)
    shutil.copyfile(path_res, end_res)

    '''# Spin Up Docker Container, Takes a maximum of 2 hours to finish
    docker_path = os.path.join(NAS_MOUNT_VOLUME,'AMR')
    docker_user_path = '%s/%s' % (amr_obj.user, amr_obj.id)
    print "Starting ARMI Analysis"
    docker_spinner(docker_path, obj_id, 'a', docker_user_path, 0, 'genesipprv2node', 12, 48000)



    print "Saving ARMI Results Now"
    for root, dirs, files in os.walk(data_path, topdown=False):
        for name in files:
            if 'results.tsv' in str(name):
                shutil.move(os.path.join(root,name),data_path)
                amr_obj.result.name = os.path.join(data_path, name)
                amr_obj.save()

    print "Starting SRST2 AMR Analysis"
    docker_path = NAS_MOUNT_VOLUME + 'AMR/%s/%s' % (amr_obj.user, amr_obj.id)
    docker_spinner(docker_path, obj_id, 'AMR', 0, 0, 'srst2', 12, 12000)
    '''

    call(['docker', 'run', '-v', os.path.abspath(data_path)+':/app/documents', '-e', 'INPUT=app/documents', '-e',
          'VAR1=AMR', '-e', 'VAR2=10', 'srst2'])

    print "Saving SRST2 Results Now"
    result_file = glob.glob(os.path.join(data_path, '*fullgenes*.txt'))
    if len(result_file) > 0:
        for result in result_file:
            if "ARGannot" in result:
                amr_obj.result.name = os.path.join(data_path, 'AMR__fullgenes__ARGannot__results.txt')
            elif "ResFinder" in result:
                amr_obj.result3.name = os.path.join(data_path, 'AMR__fullgenes__ResFinder__results.txt')

        project_obj.amr_results = 'Done'
        project_obj.save()

    else:
        print "No Results Found :("
        project_obj.amr_results = 'No Results'
        project_obj.save()
        amr_obj.error = "No Results"
        amr_obj.save()

    amr_obj.job_id = ''
    amr_obj.save()

    print "Removing Temporary Files"
    for root, dirs, files in os.walk(data_path, topdown=False):
        for name in files:
            if 'results' in str(name):
                print "Not Going to Delete", name
            else:
                os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
