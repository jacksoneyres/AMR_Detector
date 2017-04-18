# Python Library
from __future__ import absolute_import
from celery import Celery
from subprocess import call, check_output
import shutil
import os
import glob
# Django Model Imports
from SilentD.models import Project


# General Settings
app = Celery('tasks',
             backend='djcelery.backends.database.DatabaseBackend',
             broker='amqp://guest:guest@rabbit:5672//')


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
                distances = check_output(['./mash', 'dist', data.file.name, "RefSeq.msh"]).splitlines()
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

                call(['GeneSeekr', '-o', working_dir, '-m', "NCBI_AMR.fasta", "-c", "90", working_dir])

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
            distances = check_output(['./mash', 'dist', data_files[0].file.name, "RefSeq.msh"]).splitlines()
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

                call(['srst2', '--input_pe', data_files[0].file.name, data_files[1].file.name, '--output', os.path.join(working_dir, str(project_obj.id)), '--gene_db', "NCBI_AMR_SRST2.fasta"])

                # Save Results, Check file exists, if so, make sure it actually contains results
                result_file = glob.glob(os.path.join(working_dir, '*fullgenes*.txt'))
                if len(result_file) == 1:
                    print "SRST2 File Detected"
                    project_obj.srst2_results.name = result_file[0]
                    project_obj.amr_results = "Success"
                    project_obj.save()

                print "Removing Temporary Files"
                for root, dirs, files in os.walk(working_dir, topdown=False):
                    for name in files:
                        if 'fullgenes' in str(name):
                            print "Not Going to Delete", name
                        else:
                            os.remove(os.path.join(root, name))

        except Exception as e:
            print "Error, SRST2 failed!", e.__doc__, e.message
            project_obj.amr_results = "Error"
