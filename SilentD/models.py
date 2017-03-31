from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
import datetime

def create_profile(**kwargs):
    """Creates a Profile automatically when User created

    :param kwargs: contains the created signal
    """
    print kwargs
    user = kwargs["instance"]
    if kwargs["created"]:
        profile = Profile(user=user)
        profile.save()

post_save.connect(create_profile, sender=User)


class Profile(models.Model):
    """Extends Django User Profile.

    Certain features of the website check this model access priviledges. Any kind of field can be added to further
    store information on user. Defaults are created whenever a new user is created
    """
    user = models.OneToOneField(User)

    rank_choices = (
        ('Diagnostic', 'Diagnostic'),
        ('Research', 'Research'),
        ('Manager', 'Manager'),
        ('Quality', 'Quality'),
        ('Super', 'Super'),
    )
    rank = models.CharField(max_length=100, choices=rank_choices, default='Diagnostic')
    cfia_access = models.BooleanField(default=False)
    lab_choices = (
        (1, 'St-Johns'),
        (2, 'Dartmouth'),
        (3, 'Charlottetown'),
        (4, 'St-Hyacinthe'),
        (5, 'Longeuil'),
        (6, 'Fallowfield'),
        (7, 'Carling'),
        (8, 'Greater Toronto Area'),
        (9, 'Winnipeg'),
        (10, 'Saskatoon'),
        (11, 'Calgary'),
        (12, 'Lethbridge'),
        (13, 'Burnaby'),
        (14, 'Sidney'),
        (15, 'Other')
    )
    lab = models.CharField(max_length=100, choices=lab_choices, blank=True)

    # Override the __unicode__() method to return out something meaningful!
    def __unicode__(self):

        s = "%s : %s : %s" % (self.user.username, self.rank, self.lab)
        return s


class Base(models.Model):
    """Abstract model defining common fields for files and project models"""
    date = models.DateTimeField(auto_now_add=True)
    user = models.CharField(max_length=200, default=" ")
    description = models.CharField(max_length=200, blank=True)

    # This makes the class abstract and not instantiate a model of BASE
    class Meta:
        abstract = True


def generate_path(self, filename):
    """Function for file uploads,

    Path can only be generated after the model has been created and saved. Every file is uploaded to a unique folder to
    prevent file name collisions rather than allow Django to rename the files itself which it will do

    Args:
        self: Django object model
        filename: Name of the file. Path contains
    """
    object_type = self.__class__.__name__
    path = "documents/Files/tmp"
    today = datetime.datetime.now()
    unique_path = today.strftime("%Y%m%d") + "/" + str(self.id)
    if 'Data' in object_type:
        path = "documents/Files/%s/%s/%s" % (self.user, unique_path, filename)
    return path


class Data(Base):
    """Every uploaded file from File Upload view uses this model"""
    file = models.FileField(upload_to=generate_path, blank=True)
    name = models.CharField(max_length=200, blank=True)
    type = models.CharField(max_length=20, blank=True)


class Project(Base):
    """Projects are a collection of FastQ files or a Fasta for analysis"""
    files = models.ManyToManyField(Data)  # List of FastQ (Data) files
    num_files = models.IntegerField(default=0)
    organism = models.CharField(max_length=200, blank=True)
    reference = models.CharField(max_length=200, blank=True)
    type = models.CharField(max_length=20, blank=True)
    amr_results = models.CharField(max_length=50, blank=True)
    geneseekr_results = models.FileField(blank=True, null=True)
    srst2_results = models.FileField(blank=True, null=True)
