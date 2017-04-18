import csv
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
from .forms import UserForm
# Database Models
from .models import Data
from .models import Project
from .models import Profile

# Celery Tasks
from .tasks import amr_task


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
            fastq_list = Data.objects.filter(user=username, type='FastQ').order_by('-date')

            newdoc = Data(user=username, type='FastQ')
            newdoc.save()
            newdoc.file = request.FILES['file']
            newdoc.save()
            newdoc.name = newdoc.file.name.split('/')[-1]
            newdoc.save()

            # Try to find a matching fastq in database to merge the pair into a project. This is done by comparing the
            # file name with the corresponding R1 and R2 values so see if they match. The most recently uploaded files
            # will be matched and script will exit loop and proceed

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
                            # using mash

                            new_project = Project(user=username, description=file_name_1)
                            new_project.save()
                            new_project.files.add(newdoc)
                            new_project.files.add(fastq)
                            new_project.num_files = 2
                            new_project.description = file_name_1
                            new_project.type = 'fastq'
                            new_project.save()
                            # Start the automatic analysis now that a match has been found
                            amr_task.delay(new_project.id)
                            return render(request, 'SilentD/file_upload.html', {})

            print 'No Match Found'

        elif '.fasta' in file_name:
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
def amr(request):
    # Project.objects.all().delete()
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

            if 'GeneSeekr' or "SRST2" in path:
                json_dict = {}
                with open("AMR_Data.json") as f:
                    json_dict = json.loads(f.read())
                if "GeneSeekr" in path:
                    with open(proj_obj.geneseekr_results.name) as g:
                        reader = csv.DictReader(g)
                        result = {}
                        for row in reader:
                            for key, value in row.iteritems():
                                result[str(key).lstrip()] = str(value).replace("%", "")
                        result.pop("Strain")
                elif "SRST2" in path:
                    with open(proj_obj.srst2_results.name) as g:
                        result = {}
                        lines = g.readlines()
                        lines.pop(0)
                        for line in lines:
                            line = line.split("\t")
                            result[line[3]] = 100.0 - float(line[8])
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
                        rarity = json_dict[key][rarity_name]
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

    all_projects = Project.objects.filter(user=username)
    return render(request, 'SilentD/amr.html', {'projects': all_projects })
