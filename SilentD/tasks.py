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
from SilentD.models import Data
from SilentD.models import Project
# Functions
from SilentD.functions import primer_validator_function
from SilentD.functions import docker_spinner
from SilentD.functions import determine_organism
from SilentD.functions import srst2_formatter
# Other Libraries
from Bio import SeqIO

# Development Boolean
DEV = True
DOCKER_REGISTRY = "192.168.1.5:5000"
# General Settings
app = Celery('tasks',
             backend='djcelery.backends.database.DatabaseBackend',
             broker='amqp://guest:guest@localhost:5672//')


NAS_MOUNT_VOLUME = "/home/ubuntu/nas0/Genomics_Portal/documents_test/"


@app.task(bind=True)
def gene_seeker_task(self, obj_id):
    ''' GeneSeekR Performs gene detection on fastq and fasta files. It uses either SRST2 for FastQ, or GeneSeekR for
        fasta. This function is divided into a fastq path, and two fasta paths (one for custom genomes, the other for
        the premade databases
    :param self:
    :param obj_id: ID of the GeneS object
    :return: None
    '''
    print self

    # Retrieve object from database
    obj = GeneS.objects.get(id=obj_id)
    print "Starting GeneSeekR Task %s for User: %s" % (obj_id, obj.user)

    # Prepare working directory
    working_dir = 'documents/GeneSeeker/%s/%s' % (obj.user, obj.id)
    target_path = os.path.join(working_dir, 'targets')
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    genes = obj.genes
    cutoff = obj.cutoff
    organism = obj.organism

    if obj.type == 'fastq':
        # Retrieve the project object associated with GeneS object, copy over every associated file to working dir and
        # perform SRST2 via a docker container

        project_obj = Project.objects.get(id=obj.job_id)
        data_files = project_obj.files.all()
        cutoff = str(100 - int(cutoff))

        print "Copying Project Files Over to GeneSeekR Working Dir"
        for f in data_files:
            path = os.path.split(f.file.name)
            end_path = os.path.join(working_dir, path[1])
            print "Copying from %s to %s" % (f.file.name, end_path)
            shutil.copyfile(f.file.name, end_path)

        if genes == "OtherTarget":
            target_start_path = srst2_formatter(obj.targets.name)
            target_name = os.path.split(target_start_path)[1]
            target_end_path = os.path.join(working_dir, target_name)
            print "Copying Custom Target File Over to GeneSeekR Working Dir"
            shutil.copyfile(target_start_path, target_end_path)
        else:
            target_start_path = 'documents/Targets/SRST2/%s_SRST2.fasta' % genes
            target_name = os.path.split(target_start_path)[1]
            target_end_path = os.path.join(working_dir, target_name)
            print "Copying Default Target File Over to GeneSeekR Working Dir"
            shutil.copyfile(target_start_path, target_end_path)

        print 'Running Gene Seeker SRST2'
        try:
            file_path = os.path.join(working_dir, '*.gz')
            file_path = os.path.abspath(file_path)
            fasta_path = os.path.join(working_dir, '*.fasta')
            fasta_path = os.path.abspath(fasta_path)
            print "New Subprocess32", file_path, fasta_path
            #subprocess32.call(['srst2', '--input_pe', file_path, '--gene_db', fasta_path, '--max_divergence', cutoff, '--output', 'GeneSeekR'], shell=True)
            os.system('srst2 --input_pe %s --gene_db %s --max_divergence %s --output GeneSeekR' % (file_path, fasta_path, cutoff))
            #call(['docker', 'run', '-v', os.path.abspath(working_dir)+':/app/documents',
            #      '-e', 'INPUT=app/documents', '-e', 'VAR1=GeneSeekR', '-e', 'VAR2='+cutoff, 'srst2'])

            print "Saving Results Now"
            # Assign file to database entry for results if it exists, and is not empty
            results_file = glob.glob(working_dir + '/*fullgenes*.txt')
            if len(results_file) == 1 and os.path.getsize(results_file[0]) != 0:
                obj.result.name = results_file[0]
                obj.job_id = ''
                obj.save()
            else:
                print "Something went wrong, failed GeneSeekR"
                obj.error = 'No Results'
                obj.save()
        except Exception as e:
            print "***ERROR", e.__doc__, e.message
            return False

    elif obj.type == 'fasta':
        if obj.job_id == '0':   # Start Premade Genomes for GeneSeekR
            genome_path = 'documents/Current_Genomes/%s' % organism
            genome_abs = os.path.abspath(genome_path)
        else:
            project_obj = Project.objects.get(id=obj.job_id)
            data_files = project_obj.files.all()
            print "Copying Project Files Over to GeneSeekR Working Dir"
            for f in data_files:
                path = os.path.split(f.file.name)
                end_path = os.path.join(working_dir, path[1])
                print "Copying from %s to %s" % (f.file.name, end_path)
                shutil.copyfile(f.file.name, end_path)
            genome_abs = os.path.abspath(working_dir)

        # Check for User Uploaded Targets or Select Premade Ones
        if genes == "OtherTarget":
            sequences = []
            target_start_path = obj.targets.name
            # Verify correctly formatted fasta file
            target_end_path = str(target_start_path).replace(".fasta", "_v.fasta")
            output_handle = open(target_end_path, "w")
            for record in SeqIO.parse(open(target_start_path, "rU"), "fasta"):
                sequences.append(record)
            if len(sequences) > 0:
                SeqIO.write(sequences, output_handle, "fasta")
                output_handle.close()
                obj.targets.name = target_end_path
                obj.save()
                target_abs = os.path.abspath(target_end_path)
            else:
                print "ERROR: Improper Formatted Fasta"
                obj.error = "Error"
                obj.save()
                return
        else:
            target_start_path = 'documents/Targets/%s.fasta' % genes
            target_abs = os.path.abspath(target_start_path)

        results_abs = os.path.abspath(working_dir)

        print 'GeneSeekr %s -m %s -o %s -c %s' % (genome_abs, target_abs, results_abs, cutoff)

        # Notoriously unreliable program
        try:

            call(['GeneSeekr', genome_abs, '-m', target_abs, '-o', results_abs, '-c', cutoff])

            print "Saving Results Now"
            # Assign file to database entry for results if it exists, and is not empty
            results_file = glob.glob(working_dir + '/*.csv')
            if len(results_file) == 1 and os.path.getsize(results_file[0]) != 0:
                obj.result.name = results_file[0]
                obj.job_id = ''
                obj.save()
            else:
                print "Something went wrong, failed GeneSeekR"
                obj.error = 'No Results'
                obj.save()

        except Exception as e:
            print "Error, GeneSeekr Failed!", e.__doc__, e.message
            obj.error = "Error"
            obj.save()

    print "Removing Temporary Files"
    # Remove temp files, just the fasta genomes. Target folder and results remain
    if obj.targets:
        user_targets = os.path.split(obj.targets.name)[1]
        for root, dirs, files in os.walk(working_dir, topdown=False):
            for name in files:
                if 'results' in str(name) or str(name) == user_targets:
                    print "Not Going to Delete", name
                else:
                    os.remove(os.path.join(root, name))
    else:
        for root, dirs, files in os.walk(working_dir, topdown=False):
            for name in files:
                if 'results' in str(name):
                    print "Not Going to Delete", name
                else:
                    os.remove(os.path.join(root, name))


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

        try:
            project_obj.amr_results = "Running"
            project_obj.save()
            distances = check_output(['./mash', 'dist', data.file.name, "documents/Mash/RefSeqMarch2017.msh"]).splitlines()
            distances_split = []
            for line in distances:
                distances_split.append(line.split("\t"))
            if len(distances_split) > 0:
                sorted_list = sorted(distances_split, key=lambda x: x[4], reverse=True)
                project_obj.reference = sorted_list[0][1]
                if "Escherichia_coli" in sorted_list[0][1]:
                    print "detected"
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

            call(['GeneSeekr', '-i', working_dir, '-o', working_dir, '-m', "documents/AMR/NCBI_AMR_170113.fasta"])

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


@app.task(bind=True)
def amr_fasta_task(self, obj_id):
    print self

    project_obj = Project.objects.get(id=obj_id)
    data_files = project_obj.files.all()



    print "Starting AMR  Task %s for User: %s" % (obj_id, project_obj.user)

    # Create a unique AMR object for every fasta file and copy over the fasta to working dir
    amr_object_list = []
    for data in data_files:
        print project_obj.description
        print len(project_obj.description)
        amr_object = AMR(user=project_obj.user, tag=project_obj.description, organism=project_obj.organism,
                         type=project_obj.type, job_id='1', project=project_obj)
        amr_object.save()
        amr_object_list.append(amr_object)

        path = data.file.name.split('/')[-1]
        working_dir = 'documents/AMR/%s/%s' % (amr_object.user, amr_object.id)
        end_path = os.path.join(working_dir, path)

        print "Copying from %s to %s" % (data.file.name, end_path)
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)
        shutil.copyfile(data.file.name, end_path)
        amr_object.genome.name = end_path
        amr_object.save()

    error_boolean = False

    for amr_object in amr_object_list:
        working_dir = 'documents/AMR/%s/%s/' % (amr_object.user, amr_object.id)

        try:
            print "Running ARMI"
            print 'python ARMI/ARMIv2.py -i %s -m ARMI/ARMI-genes.fa -o %s -t ARMI/aro3.json' \
                  % (working_dir, os.path.join(working_dir, 'ARMI'))

            armi_dir = os.path.join(working_dir, 'ARMI')
            if not os.path.exists(armi_dir):
                os.makedirs(armi_dir)

            call(['ARMI', working_dir, '-o', os.path.join(working_dir, 'ARMI')])

            # Run blast for every unzipped genome for both ARG-ANNOT and ResFinder
            print "Running BLAST"
            p = amr_object.genome.name
            p_nofasta = p.replace(".fasta", "")
            re = p_nofasta + "_arg_results.csv"
            re2 = p_nofasta + "_arg_blast.txt"
            re3 = p_nofasta + "_res_results.csv"
            re4 = p_nofasta + "_res_blast.txt"

            # Generate the Blast Reformat 10 Output for ARG-ANNOT
            with open(re, "a") as f:
                call(['blastn', '-db', 'documents/AMR/argannot', '-query', p, '-outfmt',
                      '10 qacc sacc qlen slen length nident'], stdout=f)

            # Generate the full Blast Output for ARG-ANNOT
            with open(re2, "a") as g:
                call(['blastn', '-db', 'documents/AMR/argannot', '-query', p, '-outfmt', '0'], stdout=g)

            # Generate the Blast Reformat 10 Output for ResFinder
            with open(re3, "a") as h:
                call(['blastn', '-db', 'documents/AMR/resfinder', '-query', p, '-outfmt',
                      '10 qacc sacc qlen slen length nident'], stdout=h)

            # Generate the full Blast Output for ResFinder
            with open(re4, "a") as i:
                call(['blastn', '-db', 'documents/AMR/resfinder', '-query', p, '-outfmt', '0'], stdout=i)

            # Save BLAST result file locations to database and turn off running task integer.
            # Only add results if file not empty
            print "Saving Results to Database"
            if os.stat(re).st_size != 0:
                amr_object.result.name = re
            amr_object.result2.name = re2

            if os.stat(re3).st_size != 0:
                amr_object.result3.name = re3
            amr_object.result4.name = re4

            # Save ARMI Results, Check file exists, if so, make sure it actually contains results
            armi_result_path =  os.path.join(working_dir,'ARMI')
            print armi_result_path
            file_list = glob.glob(os.path.join(armi_result_path, 'ARMI_results*.json'))
            if len(file_list) == 1:
                contains_results = False
                #with open(file_list[0], 'rt') as f:
                #    lines = f.readlines()
                #    for line in lines:
                #        if '+' in line:
                #            contains_results = True
                            #break
                #if contains_results:
                print "Found Some ARMI Hits"
                amr_object.result5.name = file_list[0]

            amr_object.job_id = ""
            amr_object.save()

            '''print "Removing Temporary Files"
            filelist = glob.glob(working_dir + '/*')
            for name in filelist:
                # Permanently Delete all temporary files and the copied FastQ files
                if '.fasta' in name:
                    os.remove(name)
            tmp_path = os.path.join(armi_result_path, 'tmp')
            shutil.rmtree(tmp_path)'''

        except Exception as e:
             error_boolean = True
             print "Error, Blastn failed!", e.__doc__, e.message
             amr_object.error = "Error, Something Went Wrong"
             amr_object.save()

    # Wrap up Project completion indicator
    if error_boolean:
        project_obj.amr_results = "Error"
    else:
        project_obj.amr_results = "Done"
    project_obj.save()


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
    end_arg = os.path.join(data_path,'ARGannot.fasta')
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


@app.task(bind=True)
def gene_seeker_fastq_task(self, obj_id):
    print self

    # Retrieve objects and their respective files from database
    gene_obj = GeneS.objects.get(id=obj_id)
    print "Starting GeneSeekR FastQ Task %s for User: %s" % (obj_id, gene_obj.user)

    project_obj = Project.objects.get(id=gene_obj.job_id)
    data_files = project_obj.files.all()

    # Internal Folder Structure of the Website
    data_path = 'documents/GeneSeeker/%s/%s' % (gene_obj.user, gene_obj.id)
    target_path = 'documents/GeneSeeker/%s/%s/targets/' % (gene_obj.user, gene_obj.id)

    # Docker Path is the location folder to mount into the docker container
    docker_path = "%s/GeneSeeker/%s/%s" % (NAS_MOUNT_VOLUME, gene_obj.user, gene_obj.id)

    print "Copying FastQ Files Over to GeneSeeker Working Dir at %s" % docker_path
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    for data in data_files:
        path = data.file.name.split('/')[-1]
        end_path = data_path + '/' + path
        print "Copying from %s to %s" % (data.file.name, end_path)
        shutil.copyfile(data.file.name, end_path)

    print "Copying Fasta Databases Over to GeneSeeker Working Dir"
    if gene_obj.genes == 'OtherTarget':
        path_targets = gene_obj.usertargets.name

        # Unzip any files
        print "Unzipping Files"
        if 'zip' in path_targets:
            with zipfile.ZipFile(path_targets, 'r') as myzip:
                myzip.extractall(target_path)

        # Reformat Fasta Files to Accomodate SRST2
        print "Formatting Fasta Files"
        filelist = glob.glob(target_path + '/*.fasta')
        count = 0
        for f in filelist:
            genomename = f.split("/")[-1].split(".")[0]
            handle = open(f)
            records = SeqIO.parse(handle, 'fasta')
            for record in records:
                record.id = record.id.replace("__", "")
                record.id = str(count) + "__" + genomename + "__" + str(count) + "__" + "1" + "__" + record.id
                count += 1
                SeqIO.write(record, f, 'fasta')
            handle.close()

        # Add all fasta files together into 1 file
        print "Merging Files"
        file_name = gene_obj.usertargets.name
        file_name = file_name.split("/")[-1].split(".")[0]
        outfilename = target_path + '/%s_DB.fasta' % file_name
        filelist = glob.glob(target_path + '/*.fasta')
        if len(filelist) > 0:
            with open(outfilename, 'w') as outfile:
                for fname in filelist:
                    with open(fname) as infile:
                        for line in infile:
                            outfile.write(line)
        else:
            print 'No Fasta Files Present!'

        end_path = data_path + '/%s_DB.fasta' % file_name
        shutil.copyfile(outfilename, end_path)

    # Otherwise use the preformatted virulence databases (EColi, Salmonella, Listeria) only
    else:
        path_targets = 'documents/Targets/SRST2/%s_SRST2.fasta' % gene_obj.genes
        end_path = data_path + '/%s_SRST2.fasta' % gene_obj.genes
        shutil.copyfile(path_targets, end_path)

    print 'Running SRST2 Gene Detection'
    # Spin Up Docker Container, Takes a maximum of 2 hours to finish
    #docker_spinner(docker_path, obj_id, 'GeneSeekR', 0, 0, 'srst2', 6, 6000)
    call(['docker', 'run', '-e', 'VAR1=GeneSeekR', '-e', 'INPUT=/app/documents', '-v', os.path.abspath(data_path)+':/app/documents', '--rm=True', '192.168.1.5:5000/srst2'])

    print "Saving Results Now"
    result_file = glob.glob(os.path.join(data_path, '*fullgenes*.txt'))
    if len(result_file) > 0:
        gene_obj.result.name = result_file[0]

        # If this is the first time Project has been made, assign first results here
        if project_obj.geneseekr_results == "False":
            print "Saving Results For the First Time"
            project_obj.geneseekr_results = gene_obj.id
            project_obj.save()
    else:
        print "No Results Found :("
        project_obj.geneseekr_results = 'None'
        project_obj.save()

    gene_obj.job_id = ''
    gene_obj.save()

    print "Removing Temporary Files"
    filelist = glob.glob(data_path + '/*')
    for name in filelist:
        # Permanently Delete all temporary files and the copied FastQ files
        if 'results' not in name:
            os.remove(name)
