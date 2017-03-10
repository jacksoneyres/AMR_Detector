import csv
import os
from collections import defaultdict
import json
# Django Related Imports
from django.shortcuts import render
from django.contrib.auth import authenticate
from django.contrib.auth import login
from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseRedirect
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from django.utils import timezone
from .forms import UserForm
# Database Models
from .models import Data
from .models import Project
from .models import Profile

# Celery Tasks
from .tasks import app
from .tasks import gene_seeker_task
from .tasks import amr_fasta_task
from .tasks import amr_fastq_task
from .tasks import amr_task
from .tasks import gene_seeker_fastq_task

# Other Functions
from Bio.SeqUtils import MeltingTemp
from functions import armi_rarity
from functions import determine_organism
import shutil
import datetime

# Create your views here.
def register(request):
    # If it's a HTTP POST, we're interested in processing form data.
    if request.method == 'POST':
        print request.POST
        user_form = UserForm(data=request.POST)

        if user_form.is_valid():
            # Save the user's form data to the database.
            user = user_form.save()

            # Now we hash the password with the set_password method.
            # Once hashed, we can update the user object.
            user.set_password(user.password)
            user.save()

            print user
            user_info = Profile(rank='Research')
            user_info.user = user
            user_info.save()

            print user.profile

            # Auto Log in the new user into system
            new_user = authenticate(username=request.POST['username'],
                                    password=request.POST['password'])
            login(request, new_user)

            # Redirect to main index page
            return HttpResponseRedirect('/bio/index/')
        # Invalid form or forms - mistakes or something else?
        # Print problems to the terminal.
        # They'll also be shown to the user.
        else:
            print user_form.errors

    # Something went wrong, redirect back to login page

    messages.add_message(request, messages.INFO, 'Form Errors or User Already Exists')
    return render(request, 'SilentD/login.html', {})


def user_login(request):
    # If the request is a HTTP POST, try to pull out the relevant information.
    if request.method == 'POST':
        # Gather the username and password provided by the user.
        # This information is obtained from the login form.
        # We use request.POST.get('<variable>') as opposed to request.POST['<variable>'],
        # because the request.POST.get('<variable>') returns None, if the value does not exist,
        # while the request.POST['<variable>'] will raise key error exception
        username = request.POST.get('username')
        password = request.POST.get('password')

        # Use Django's machinery to attempt to see if the username/password
        # combination is valid - a User object is returned if it is.
        user = authenticate(username=username, password=password)

        # If we have a User object, the details are correct.
        # If None (Python's way of representing the absence of a value), no user
        # with matching credentials was found.
        if user:
            print user
            # Is the account active? It could have been disabled.
            if user.is_active:
                # If the account is valid and active, we can log the user in.
                # We'll send the user back to the homepage.
                login(request, user)
                return HttpResponseRedirect('/bio/index/')
            else:
                # An inactive account was used - no logging in!
                return HttpResponse("Your account is disabled.")
        else:
            # Bad login details were provided. So we can't log the user in.
            error_message = "Invalid Login Details Provided"
            messages.add_message(request, messages.ERROR, error_message)

            print "Invalid login details: {0}, {1}".format(username, password)
            return render(request, "SilentD/login.html", {})

    # The request is not a HTTP POST, so display the login form.
    # This scenario would most likely be a HTTP GET.
    else:
        # No context variables to pass to the template system, hence the
        # blank dictionary object...
        return render(request, 'SilentD/login.html', {})


# Use the login_required() decorator to ensure only those logged in can access the view.
@login_required
def user_logout(request):
    # Since we know the user is logged in, we can now just log them out.
    logout(request)

    # Take the user back to the homepage.
    return HttpResponseRedirect('/')


# csrf_exempt decorator allowing easier Dropzone.js compatibility.
@csrf_exempt
@login_required
def file_upload(request):
    username = None
    if request.user.is_authenticated():
        username = request.user.username

    if request.method == 'POST':
        print "User %s uploading %s " % (username, request.FILES)
        # Create a new database entry for the file
        file_name = str(request.FILES['file'])
        # Save model first to generate ID, then upload the file to the folder with ID
        if 'fastq.gz' in file_name:
            newdoc = Data(user=username, type='FastQ')
            newdoc.save()
            newdoc.file = request.FILES['file']
            newdoc.save()
            newdoc.name = newdoc.file.name.split('/')[-1]
            newdoc.save()

            '''# Try to find a matching fastq in database to merge the pair into a project. This is done by comparing the
            # file name with the corresponding R1 and R2 values so see if they match. The most recently uploaded files
            # will be matched and script will exit loop and proceed
            fastq_list = Data.objects.filter(user=username, type='FastQ').order_by('-date')

            file_name_split = newdoc.name.split('_')
            if len(file_name_split) < 2:
                return render(request, 'SilentD/file_upload.html', {})
            else:
                file_name_1 = file_name_split[0]
                if '_R1' in newdoc.name:
                    r_value_1 = 'R1'
                elif 'R2' in newdoc.name:
                    r_value_1 = 'R2'
                else:
                    pass
                    # Improperly named file, error error

                # Search for a corresponding file that matches
                for fastq in fastq_list:
                    file_name_2 = fastq.name.split('_')[0]
                    if '_R1' in fastq.name:
                        r_value_2 = 'R1'
                    elif '_R2' in fastq.name:
                        r_value_2 = 'R2'

                    if file_name_1 == file_name_2:
                        if (r_value_1 == 'R1' and r_value_2 == 'R2') or (r_value_1 == 'R2' and r_value_2 == 'R1'):
                            print "Found A Match!"
                            # Create a new project database entry with the two matched files, organism to be determined
                            # using S16 methods.

                            # # Only create a project if another one of same name not created within 10 seconds
                            # existing_project = Project.objects.filter(user=username, description=file_name_1)
                            # if len(existing_project) > 0:
                            #     td = (datetime.datetime.now() - existing_project[0].date).total_seconds()
                            #     if td < 10:
                            #         return render(request, 'SilentD/file_upload.html', {})
                            # else:
                            new_project = Project(user=username, description=file_name_1)
                            new_project.save()
                            new_project.files.add(newdoc)
                            new_project.files.add(fastq)
                            new_project.num_files = 2
                            new_project.description = file_name_1
                            new_project.type = 'Individual'
                            new_project.save()
                            # Start the automatic analysis now that a match has been found
                            # pipeline_task.delay(new_project.id)
                            return render(request, 'SilentD/file_upload.html', {})

            print 'No Match Found'
            '''

        elif '.fa' in file_name:
            # Database entry must be saved first to generate unique ID
            newdoc = Data(user=username, type='Fasta')
            newdoc.save()
            # Upload the file to database entry and corresponding unique folder
            newdoc.file = request.FILES['file']
            newdoc.save()
            newdoc.name = newdoc.file.name.split('/')[-1]
            newdoc.save()

            new_project = Project(user=username, description=newdoc.name, organism="Temp")
            new_project.save()
            new_project.files.add(newdoc)
            new_project.num_files = 1
            new_project.type = 'fasta'
            new_project.save()

            # Start the automatic analysis
            amr_task.delay(new_project.id)
    return render(request, 'SilentD/file_upload.html', {})


@login_required
def data(request):
    # AMR.objects.all().delete()
    # GeneS.objects.all().delete()
    # Project.objects.all().delete()
    # MiSeq.objects.all().delete()
    # Data.objects.all().delete()
    username = ''
    if request.user.is_authenticated():
        username = request.user.username

    project_creation_fail = False

    if request.method == 'POST':
        print request.POST
        spades_id = request.POST.get('spades')

        # Handle a new assignment of an organism, or 16S Typing
        if "assign_organism" in request.POST and "select_organism" in request.POST:
            assign_organism = request.POST.get('assign_organism')
            pro_obj = Project.objects.get(id=assign_organism)
            if request.POST.get('select_organism') == '16S':
                pro_obj.organism = request.POST.get('select_organism')
                pro_obj.save()
                pipeline_task.delay(assign_organism)
            else:
                pro_obj.organism = request.POST.get('select_organism')
                pro_obj.save()

            print "Assigned Organism"
        # Request to view SPAdes results from a previous run

        # Perform analysis of a project, a variety of jobs available
        elif "job" in request.POST:
            job = request.POST.get('job')
            proj_id = request.POST.get('project_id')
            pro_obj = Project.objects.get(id=proj_id)

            if job == 'spades_start':
                # Turn on Spades Running Flag for Template and run SPAdes task
                pro_obj.spades_results = "Running"
                pro_obj.save()
                spades_task.delay(proj_id)

            elif job == 'amr_start':
                pro_obj.amr_results = "Running"
                pro_obj.save()
                if pro_obj.type == 'fasta':
                    amr_fasta_task.delay(proj_id)
                elif pro_obj.type == 'fastq':
                    amr_fastq_task.delay(proj_id)

            elif job == 'mlst_start':
                # Verify the organism is one of the limited supporting ones
                organism = determine_organism(pro_obj.organism, 'MLST')
                if organism:
                    pro_obj.mlst_results = "Running"
                    pro_obj.save()
                    mmlst_task.delay(proj_id)
                else:
                    error_message = "Error! No Matching MLST Profile for Organism: %s" % pro_obj.organism
                    messages.add_message(request, messages.ERROR, error_message)

        elif "spades_results" in request.POST:
            proj_id = request.POST.get('spades_results')
            pro_obj = Project.objects.get(id=proj_id)
            return render(request, 'SilentD/spades_results.html', {'document': pro_obj})

        elif "amr_results" in request.POST:
            proj_id = request.POST.get('amr_results')
            pro_obj = Project.objects.get(id=proj_id)

            documents = AMR.objects.filter(user=username, project=pro_obj)
            return render(request, 'SilentD/amr.html', {'documents': documents})

        elif "mlst_results" in request.POST:
            proj_id = request.POST.get('mlst_results')
            pro_obj = Project.objects.get(id=proj_id)

            documents = MLST.objects.filter(user=username, project=pro_obj)
            return render(request, 'SilentD/mlst.html', {'documents': documents})

        elif "delete" in request.POST:
            proj_id = request.POST['delete']
            pro_obj = Project.objects.get(id=proj_id)
            amr_objs = AMR.objects.filter(project=pro_obj)
            for amr in amr_objs:
                amr.delete()
            mlst_objs = MLST.objects.filter(project=pro_obj)
            for mlst in mlst_objs:
                mlst.delete()
            pro_obj.delete()


    # Retrieve all uploaded files relating to the user
    indiv_projects = Project.objects.filter(user=username, type='Individual')
    fastq_projects = Project.objects.filter(user=username, type='fastq')
    fasta_projects = Project.objects.filter(user=username, type='fasta')
    all_projects =  Project.objects.all()
    return render(request, 'SilentD/data.html', {'indiv_projects': indiv_projects,
                                                 'fastq_projects': fastq_projects,
                                                 'fasta_projects': fasta_projects,
                                                 'all_projects' : all_projects,
                                                })


@login_required
def create_project(request):

    # AMR.objects.all().delete()
    # GeneS.objects.all().delete()
    # Project.objects.all().delete()
    # Data.objects.all().delete()
    username = ''
    if request.user.is_authenticated():
        username = request.user.username

    project_creation_fail = False

    if request.method == 'POST':

        print request.POST
        if 'project' in request.POST:
            ids = request.POST.get("ids")
            print ids
            name_list = json.loads(ids)
            print name_list
            project_files_fastq = []
            project_files_fasta = []
            now = timezone.now()
            delta = datetime.timedelta(hours=2)
            recent_files =  Data.objects.filter(user=username).order_by('-date')
            for file in recent_files:
                if (now - file.date) < delta:
                    if file.name in name_list:
                        print file.type
                        if file.type == "Fastq":
                            project_files_fastq.append(file)
                        elif file.type == "Fasta":
                            print "fasta"
                            project_files_fasta.append(file)
                    print "Recent", file.date
                else:
                    print "Not", file.date

            if len(project_files_fasta) > 1:
                print "MAKING pROJECT"
                new_project = Project(user=username, description=now)
                new_project.save()
                for obj in project_files_fasta:
                    new_project.files.add(obj)
                new_project.num_files = len(project_files_fasta)
                new_project.type = "fasta"
                new_project.save()

        else:
            description = request.POST.get('name')
            organism = request.POST.get('organism')
            if request.POST.get('ids'):
                # FastQ
                data_file_list = request.POST.get('ids')
            else:
                #Fasta
                data_file_list = request.POST.get('ids2')

            project_type = request.POST.get('type')

            if data_file_list and organism and description and project_type:
                data_file_list2 = data_file_list.replace('id=', '')
                data_list = data_file_list2.split('&')

                data_obj_list = []
                filename_list = []
                failed_list = []
                for item in data_list:
                        data_obj = Data.objects.get(id=item)
                        data_obj_list.append(data_obj)
                        filename_list.append(str(data_obj.name))

                if project_type == 'fastq':
                    # Conditions below are tested in order for files to be added to a project
                    # File count is an even number
                    # Files have proper formatted _R1 or _R2 in file name
                    # Each file is paired with its other R value

                    if len(data_list) % 2 != 0:
                        # List has uneven number of files due retrieval error
                        project_creation_fail = True
                    else:
                        # Create a dictionary of strain names, and populate with found R values
                        file_dict = defaultdict(list)
                        for filename in filename_list:
                            name = filename.split("_")[0]
                            if '_R1' in filename:
                                rvalue = 'R1'
                                file_dict[name].append(rvalue)
                            elif '_R2' in filename:
                                rvalue = 'R2'
                                file_dict[name].append(rvalue)
                            else:
                                error_message = "Error! File %s does not have a correct RValue in the format _R1 or _R2" \
                                                 % name
                                messages.add_message(request, messages.ERROR, error_message)

                        # Verify all files are paired and have a match of R1 and R2, not R1,R1, R2,R2 or only 1 R value

                        for key, value in file_dict.items():
                            if len(value) == 2:
                                if (value[0] == "R1" and value[1] == "R2") or (value[0] == "R2" and value[1] == "R1"):
                                    print "Match!"
                                else:
                                    project_creation_fail = True
                                    failed_list.append(key)
                                    error_message = "Error! %s has two R1 or two R2 values" % key
                                    messages.add_message(request, messages.ERROR, error_message)
                            else:
                                project_creation_fail = True
                                failed_list.append(key)
                                error_message = "Error! 2 Files must be associated with %s" % key
                                messages.add_message(request, messages.ERROR, error_message)

                if project_creation_fail:
                    error_message = "Error! No paired match found for the Following:  " + ", ".join(failed_list) + \
                                    ", Ensure each pair of files contains *_R1_001.fastq.gz and *_R2_001.fastq.gz"
                    messages.add_message(request, messages.ERROR, error_message)
                else:
                    # Create a Fasta or Fastq Project
                    new_project = Project(user=username, description=description, organism=organism)
                    new_project.save()
                    for obj in data_list:
                        new_project.files.add(obj)
                    new_project.num_files = len(data_list)
                    new_project.type = request.POST.get('type')
                    new_project.save()

                    success_message = description + " created succesully"
                    messages.add_message(request, messages.SUCCESS, success_message)

                    # Send user to the Projects main page
                    indiv_projects = Project.objects.filter(user=username, type='Individual')
                    fastq_projects = Project.objects.filter(user=username, type='fastq')
                    fasta_projects = Project.objects.filter(user=username, type='fasta')
                    return render(request, 'SilentD/data.html', {'indiv_projects': indiv_projects,
                                                                 'fastq_projects': fastq_projects,
                                                                 'fasta_projects': fasta_projects})

    # Retrieve all uploaded files relating to the user
    documents = Data.objects.filter(user=username).exclude(file__isnull=True).exclude(file="")
    fastqs = Data.objects.filter(user=username, type='FastQ').exclude(file__isnull=True).exclude(file="")
    fastas = Data.objects.filter(user=username, type='Fasta').exclude(file__isnull=True).exclude(file="")
    # Convert file size to megabytes
    for d in fastqs:
        if d.file:
            if os.path.isfile(d.file.name):
                d.size = d.file.size/1000/1000
            else:
                # For some reason the file has been deleted, update the databases to remove this entry
                Data.objects.get(id=d.id).delete()
        else:
            d.size = 0
    for d in fastas:
        if d.file:
            if os.path.isfile(d.file.name):
                d.size = float(d.file.size)/1000.0/1000.0
            else:
                # For some reason the file has been deleted, update the databases to remove this entry
                Data.objects.get(id=d.id).delete()
        else:
            d.size = 0
    return render(request, 'SilentD/create_project.html', {'documents': documents, 'fastqs': fastqs, 'fastas': fastas})


@login_required
def amr(request):
    #Project.objects.all().delete()
    username = ''
    if request.user.is_authenticated():
        username = request.user.username

    if request.POST:
        print request.POST
        # Send back the result file in a table, either ARG-ANNOT or ResFinder
        if 'result' in request.POST:
            path = request.POST['result']
            proj_id = request.POST['id']
            # Retrieve all past jobs to send to page
            proj_obj = Project.objects.get(id=proj_id)

            data_list = []

            # Form data is the path to result file


            if 'GeneSeekr' in path:
                json_dict = {}
                with open("documents/AMR/AMR_Data.json") as f:
                    json_dict = json.loads(f.read())
                    #print json_dict
                with open(proj_obj.geneseekr_results.name) as g:
                    reader = csv.DictReader(g)
                    result = {}
                    for row in reader:
                        for key, value in row.iteritems():
                            result[str(key).lstrip()] = str(value).replace("%", "")
                    result.pop("Strain")
                    #print result
                display_dict = {}
                if "Escherichia_coli" in proj_obj.reference:
                    rarity_name = "ECOLI"
                    organism = "Escherichia_coli"
                elif "Listeria_monocytogenes" in proj_obj.reference:
                    rarity_name = "LISTERIA"
                    organism = "Listeria_monocytogenes"
                elif "Salmonella_enterica" in proj_obj.reference:
                    rarity_name = "SALMONELLA"
                    organism = "Salmonella_enterica"
                elif "Shigella_boydii" in proj_obj.reference:
                    rarity_name = "SHIGELLA_B"
                    organism = "Shigella_boydii"
                elif "Shigella_sonnei" in proj_obj.reference:
                    rarity_name = "SHIGELLA_S"
                    organism = "Shigella_sonnei"
                elif "Shigella_flexneri" in proj_obj.reference:
                    rarity_name = "SHIGELLA_F"
                    organism = "Shigella_flexneri"
                elif "Shigella_dysenteriae" in proj_obj.reference:
                    rarity_name = "SHIGELLA_D"
                    organism = "Shigella_dysenteriae"
                elif "Vibrio_parahaemolyticus" in proj_obj.reference:
                    rarity_name = "VIBRIO"
                    organism = "Vibrio_parahaemolyticus"
                elif "Yersinia_enterocolitica" in proj_obj.reference:
                    rarity_name = "YERSINIA"
                    organism = "Yersinia_enterocolitica"
                elif "Campylobacter_coli" in proj_obj.reference:
                    rarity_name = "CAMPY_COLI"
                    organism = "Campylobacter_coli"
                elif "Campylobacter_jejuni" in proj_obj.reference:
                    rarity_name = "CAMPY_JEJUNI"
                    organism = "Campylobacter_jejuni"
                else:
                    rarity_name = "OTHER"
                    organism = "N/A"

                for key, value in result.items():
                    if rarity_name in json_dict[key]:
                        rarity = json_dict[key]["ECOLI"]
                    else:
                        rarity = 0
                    display_dict[key] = {"identity": value,
                                         "class": json_dict[key]["class"],
                                         "antibiotic": json_dict[key]["antibiotic"],
                                         "rarity": rarity,
                                         "annotation": json_dict[key]["annotation"]}

                    classes = set()
                    results_dict = {}
                    for key, value in display_dict.items():
                        classes.add(value["class"])
                    for item in classes:
                        results_dict[item] = {}
                    for item in classes:
                        for key, value in display_dict.items():
                            if value["class"] == item:
                                results_dict[item][key] = value
                print results_dict
                all_projects = Project.objects.filter(user=username)
                caption = [proj_obj.description, organism]
                return render(request, 'SilentD/amr.html', {'projects': all_projects,
                                                            'results': results_dict,
                                                            "caption": caption})

                # # Create list containing description, organism species, job date and type
                # caption = [amr_object.tag, amr_object.organism, amr_object.date, amr_object.type]
                #
                # with open(path, 'rt') as f:
                #     json_dict = json.load(f)
                #     print json_dict
                #     for key, value in json_dict.items():
                #         for element in value['resist']:
                #             if element in armi_rarity:
                #                 value = armi_rarity[element]
                #                 category = value[0]
                #                 organism = determine_organism(amr_object.organism, 'MLST')
                #                 print amr_object.organism
                #                 print organism
                #                 if organism == 'Escherichia':
                #                     rarity = value[2]
                #                 elif organism == 'Salmonella':
                #                     rarity = value[3]
                #                 elif organism == 'Listeria':
                #                     rarity = value[4]
                #                 else:
                #                     rarity = 0
                #             else:
                #                 category = "Other"
                #                 rarity = 0
                #
                #             if rarity <= 10:
                #                 rarity = 5
                #             elif 10 < rarity <= 20:
                #                 rarity = 4
                #             elif 20 < rarity <= 30:
                #                 rarity = 3
                #             elif 30 < rarity <= 50:
                #                 rarity = 2
                #             elif rarity > 50:
                #                 rarity = 1
                #
                #             if category not in armi_results:
                #                 armi_results[category] = {element: rarity}
                #             else:
                #                 armi_results[category][element] = rarity


            # else:
            #
            #     if "obj" in request.POST:
            #         obj = AMR.objects.get(id=request.POST['obj'])
            #         if obj.type == 'fasta':
            #             with open(path, 'rt') as f:
            #                 lines = f.readlines()
            #                 keys = ['Strain', 'Gene', 'Query Length', 'Subject Length', 'Alignment Length', 'Matched Bases',
            #                         '% Identity']
            #
            #                 # Parses Blast Output into Datatables compatible dictionary
            #                 for line in lines:
            #                     line_list = line.split(',')
            #                     coverage = (float(line_list[5])/float(line_list[3])*100.0)
            #                     line_list.append(str(coverage))
            #                     data_list.append(line_list)
            #
            #             return render(request, 'SilentD/amr.html', {'documents': documents,
            #                                                         'results': data_list, 'keys': keys, 'display': True})
            #         elif obj.type == "fastq":
            #             with open(path, 'rt') as f:
            #                 lines = f.readlines()
            #                 keys = lines[0].split("\t")
            #                 lines.pop(0)
            #                 # Parses Blast Output into Datatables compatible dictionary
            #                 for line in lines:
            #                     line_list = line.split('\t')
            #                     data_list.append(line_list)
            #
            #             return render(request, 'SilentD/amr.html', {'documents': documents,
            #                                                         'results': data_list, 'keys': keys,
            #                                                         'display': True})
    all_projects = Project.objects.filter(user=username)
    return render(request, 'SilentD/amr.html', {'projects': all_projects })
