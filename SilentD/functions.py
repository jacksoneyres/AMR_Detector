import json
import requests
from time import sleep
from Bio.SeqUtils import GC
# from Bio.SeqUtils import MeltingTemp
from Bio import SeqIO
from Bio.Seq import translate
from subprocess import check_output


def determine_organism(project_organism, task):
    if task == "MLST":
        if "Escherichia" in project_organism:
            organism = "Ecoli"
        elif "Salmonella" in project_organism:
            organism = "Salmonella"
        elif "Listeria" in project_organism:
            organism = "Listeria"
        elif "Vibrio" in project_organism:
            organism = "Vibrio"
        elif "Campy" in project_organism:
            organism = "Campy"
        elif "Staph" in project_organism:
            organism = "Staph"
        else:
            organism = False
    return organism


def srst2_formatter(fasta_path):
     # Reformat Fasta Files to Accomodate SRST2
    print "Formatting Fasta Files"
    count = 0
    handle = open(fasta_path)
    records = SeqIO.parse(handle, 'fasta')
    new_file = str(fasta_path).replace('.fasta', '_SRST2.fasta')
    with open(new_file, 'w') as f:
        for record in records:
            record.id = record.id.replace("__", "")
            record_short = record.id[0:10]
            record.id = str(count) + "__" + record_short + "__" + str(count) + "__" + "1" + "__" + record.id
            count += 1
            SeqIO.write(record, f, 'fasta')
    handle.close()
    return new_file


def calculate_size(filename):
    handle = open(filename, 'rU')
    for record in SeqIO.parse(handle, "fasta"):
        return len(record.seq)


def translate_aa(filename):
    handle = open(filename, 'rU')
    for record in SeqIO.parse(handle, "fasta"):
        return translate(record.seq)


def calculate_gc(filename):
    pass
    # newest = max(glob.iglob('documents/fasta/*.fasta'), key=os.path.getctime)



def primer_validator_function(forward, reverse, target, mism):
    print "Running ePCR"

    list_dictionary = []
    n = 0

    # Find all possible oligos of degenerative bases
    s = check_output(['./dg', forward])
    s2 = check_output(['./dg', reverse])

    print "Forward Primers"
    print s
    print "Reverse Primers"
    print s2

    # Seperate primers into lists
    forward_primers = s.split('\n')[:-1]
    reverse_primers = s2.split('\n')[:-1]

    # Compare every forward primer against every reverse primer
    for primer_f in forward_primers:
        for primer_r in reverse_primers:

            content = []
            targets = []

            # Create a list of every hash file needed to be run against
            # Ecoli is such a huge database it must be analysed in chunks and merged together.
            # Otherwise a single hash file would take days to create and be over a terabyte in size
            if 'ecoli' in target:
                if 'cfia' in target:
                    targets.append(target)
                else:
                    targets.append('ecoli_ncbi_1.hash')
                    targets.append('ecoli_ncbi_2.hash')
                    targets.append('ecoli_ncbi_3.hash')
            else:
                targets.append(target)

            # Get STDOUT of program and split into list of lines
            for tar in targets:
                print "re-PCR -s %s -n %s -g0 -G %s %s 120-2000" % (tar, mism, primer_f, primer_r)
                output = check_output(['./re-PCR', '-s', tar, '-n '+mism, '-g0', '-G', primer_f, primer_r, '120-2000'])
                output_lines = output.splitlines()
                print len(output_lines)/4
                # Remove first and last line due to formatting from program
                output_lines.pop(0)
                output_lines.pop()
                content.extend(output_lines)

            s = ""
            for line in content:
                # Extract tab delimited data
                if line.startswith('STS-1'):
                    the_list = line.split('\t')
                    # Remove the 120-2000 from amplicon length
                    length = the_list[7].split('/')[0]
                    list_dictionary.append({'seq': the_list[1], 'strand': the_list[2], 'from': the_list[3],
                                            'to': the_list[4], 'mism': the_list[5], 'gaps': the_list[6],
                                            'length': length, 'link': ""})
                else:
                    # Extract show alignments visuals
                    if line.startswith('####'):
                        list_dictionary[n]['alignment'] = s
                        # Reset string to empty to next visual
                        s = ""
                        n += 1
                    else:
                        s += line
                        s += '\n'

    # Remove duplicate dictionaries from a list
    # Transform the inital dict in a list of tuples, put into a set (that removes duplicates entries),
    # and then back into a dict.
    result = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in list_dictionary)]

    # Add link to every result based off of NCBI data only
    for row in result:
        if row['seq'].startswith('Escherichia') \
                or row['seq'].startswith('Listeria') \
                or row['seq'].startswith('Salmonella') \
                or row['seq'].startswith('Campylobacter')\
                or row['seq'].startswith('Shigella')\
                or row['seq'].startswith('Vibrio'):
            link = 'http://www.ncbi.nlm.nih.gov/nuccore/'
            link += row['seq'].split('_')[-1]
            link += '?from='
            link += row['from']
            link += '&to='
            link += row['to']
            row['link'] = link

            # Remove the GB Number from the name
            row['seq'] = row['seq'][:-10].replace('_', ' ')

    return result


# Makes a JSON Format for Apache Mesos/Marathon that spins up existing docker containers.
def json_creator(givenpath, idname, variable1, variable2, variable3, image, cpus, memory, containerpath):
    # Create the Marathon/Mesos REST API jSON
    jsonfile = {'container': {'docker': {}}, 'env': {}}
    pass  # All this does is turn off the Pycharm warning for above line
    jsonfile["id"] = "%s-%s" % (image, idname)
    jsonfile["args"] = ["dt"]
    jsonfile["container"]["type"] = "DOCKER"
    jsonfile["container"]["docker"]["image"] = "192.168.1.5:5000/%s" % image
    jsonfile["container"]["docker"]["privileged"] = True
    jsonfile["container"]["docker"]["forcePullImage"] = True
    jsonfile["container"]["volumes"] = [{"containerPath": containerpath,
                                         "hostPath": givenpath,
                                         "mode": "RW"
                                         },
                                        {"containerPath": "/hdfs",
                                         "hostPath": "/hdfs",
                                         "mode": "RW"
                                         }]
    jsonfile["instances"] = 1
    jsonfile["env"]["INPUT"] = "/app/documents"

    # URL To Kill Docker Containers, Must Be Changed If Moving Server Frameworks
    jsonfile["env"]["ID"] = "http://192.168.1.5:8080/v2/apps/" + jsonfile['id']
    jsonfile["env"]["NFS_MOUNT"] = "192.168.1.18:/mnt/zvolume0"
    jsonfile["env"]["CONT_NAME"] = jsonfile['id']
    jsonfile["env"]["VAR1"] = str(variable1)
    jsonfile["env"]["VAR2"] = str(variable2)
    jsonfile["env"]["VAR3"] = str(variable3)
    jsonfile["cpus"] = cpus
    jsonfile["mem"] = memory
    jsonfile["uris"] = []

    # Convert Python Dict to JSON
    json_file = json.dumps(jsonfile)
    return json_file


def docker_spinner(givenpath, idname, variable1, variable2, variable3, image, cpus, memory,
                   containerpath="/app/documents"):
    print 'Running %s Docker' % image

    # Create json File
    json_file = json_creator(givenpath, idname, variable1, variable2, variable3, image, cpus, memory, containerpath)
    print json_file

    print "Spinning up Docker Container"
    # Spin Up Docker Container for SRST2
    url = 'http://192.168.1.5:8080/v2/apps'
    payload = json_file
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, data=payload, headers=headers)
    print response

    print "Waiting for Docker to Finish"
    address = 'http://192.168.1.5:8080/v2/apps/%s-%s' % (image, idname)

    # Wait for Docker Container to Finish (2 hour loop)
    count = 0
    while count != 3440:
        count += 1
        sleep(5)

        if 'does not exist' in check_output(['curl', '-X', 'GET', address]):
            print 'Docker File Finished'
            break
        else:
            print '. . . '

    # If loop maxed out, force kill the container
    if count == 1440:
        print check_output(['curl', '-X', 'DELETE', address])
        print "Docker Container %s-%s Crashed and Hanged, Force Killed" % (image, idname)
    else:
        print "Docker Container %s-%s Finished" % (image, idname)


# ARMI Data Run Against Genome Databases. Updated Nov 10, 2015
# Antibiotic Name: Category, CFIA Rarity, E.Coli Combined Rarity, Salmonella Combined Rarity, Listeria Combined Rarity


armi_rarity = {
    "acriflavin": ["acridine dye", 0.37817396, 0.0909366475, 0, 0.1092896175],
    "amikacin": ["aminoglycoside", 35.2242031334, 7.1536829342, 99.1706161137, 0.218579235],
    "aminocoumarin antibiotic": ["antibiotic molecule", 66.0723933009, 99.9696877842, 99.8815165877, 0.5464480874],
    "aminocoumarin sensitive parY": ["antibiotic sensitive DNA topoisomerase subunit parY", 0, 0, 0, 0],
    "aminoglycoside": ["antibiotic molecule", 62.3987034036, 99.6968778418, 99.8815165877, 0.4371584699],
    "apramycin": ["aminoglycoside", 0.2701242572, 3.1221582298, 2.1327014218, 0],
    "arbekacin": ["aminoglycoside", 34.3057806591, 4.7287056684, 99.1706161137, 0.1092896175],
    "astromicin": ["aminoglycoside", 0.2701242572, 3.1221582298, 2.1327014218, 0],
    "azidamfenicol": ["phenicol", 2.2150189087, 5.6683843589, 0.4739336493, 0.1092896175],
    "azithromycin": ["macrolide", 0.4321988115, 7.6386783874, 0, 0.218579235],
    "bacitracin": ["mixture", 61.0480821178, 99.8787511367, 99.8222748815, 0.5464480874],
    "bacitracin A": ["bacitracin", 61.0480821178, 99.8787511367, 99.8222748815, 0.5464480874],
    "bacitracin B": ["bacitracin", 61.0480821178, 99.8787511367, 99.8222748815, 0.5464480874],
    "bacitracin F": ["bacitracin", 61.0480821178, 99.8787511367, 99.8222748815, 0.5464480874],
    "beta-lactam": ["antibiotic molecule", 66.7747163695, 99.9696877842, 100, 0.6557377049],
    "bleomycin": ["mixture", 0, 0.3940588057, 0.1184834123, 0],
    "butirosin": ["aminoglycoside", 1.4046461372, 5.7290087905, 2.7251184834, 0.1092896175],
    "cefotaxime": ["cephalosporin", 1.2965964344, 0, 0, 0],
    "chloramphenicol": ["phenicol", 3.8897893031, 9.7302212792, 1.8957345972, 0.218579235],
    "chlortetracycline": ["tetracycline derivative", 1.8368449487, 0.4546832373, 0, 1.6393442623],
    "ciprofloxacin": ["fluoroquinolone", 95.5699621826, 99.9696877842, 99.8815165877, 98.1420765027],
    "clarithromycin": ["macrolide", 0.3241491086, 7.8508638982, 0.1184834123, 0.1092896175],
    "clindamycin": ["lincosamide", 0.0540248514, 0.2424977266, 0, 0.218579235],
    "clorobiocin": ["aminocoumarin antibiotic", 0, 0, 0, 0],
    "cloxacillin": ["penam", 60.1836844949, 99.8484389209, 99.3483412322, 0.5464480874],
    "coumermycin A1": ["aminocoumarin antibiotic", 0, 0, 0, 0],
    "dalfopristin": ["streptogramin A antibiotic", 0, 0.2424977266, 0, 0],
    "Dapsone": ["sulfones", 0, 0, 0, 0],
    "daptomycin": ["lipopeptide antibiotic", 0.3241491086, 0, 0, 0],
    "defensin": ["peptide antibiotic", 0.2160994057, 0, 0, 0],
    "demeclocycline": ["tetracycline derivative", 1.8368449487, 0.4546832373, 0, 1.6393442623],
    "diaminopyrimidine": ["antibiotic molecule", 2.4311183144, 20.885116702, 0.7109004739, 0.1092896175],
    "dibekacin": ["aminoglycoside", 34.3057806591, 4.3952712943, 99.1706161137, 0.1092896175],
    "dirithromycin": ["macrolide", 0.1080497029, 7.6386783874, 0, 0.1092896175],
    "doxycycline": ["tetracycline derivative", 1.8368449487, 0.4546832373, 0, 1.6393442623],
    "edeine": ["peptide antibiotic", 0, 0, 0, 0],
    "elfamycin": ["antibiotic molecule", 57.2663425176, 81.9036071537, 67.1800947867, 99.3442622951],
    "enacyloxin IIa": ["elfamycin", 0, 0, 0, 0],
    "enoxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "erythromycin": ["macrolide", 91.3560237709, 99.8787511367, 99.3483412322, 97.9234972678],
    "ethambutol": ["polyamine antibiotic", 0.1080497029, 0, 0, 0],
    "ethionamide": ["miscellaneous antibiotic", 0, 0, 0, 0],
    "factumycin": ["kirromycin-like antibiotics", 0, 0, 0, 0],
    "florfenicol": ["phenicol", 1.6747703944, 4.455895726, 1.5402843602, 0.1092896175],
    "fluoroquinolone": ["antibiotic molecule", 95.7320367369, 99.9696877842, 99.9407582938, 98.1420765027],
    "fosfomycin": ["miscellaneous antibiotic", 2.5391680173, 0.0909366475, 0.1184834123, 0],
    "fusidic acid": ["miscellaneous antibiotic", 1.2965964344, 0, 0, 0],
    "G418": ["aminoglycoside", 1.4046461372, 5.7593210064, 2.7251184834, 0.1092896175],
    "gatifloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "GE2270A": ["cyclic thiazolyl peptide elfamycin", 0, 0, 0, 0],
    "gentamicin B": ["aminoglycoside", 35.2242031334, 7.1536829342, 99.1706161137, 0.218579235],
    "gentamicin C": ["mixture", 0.2701242572, 3.4859048196, 2.1327014218, 0],
    "glycopeptide antibiotic": ["antibiotic molecule", 0.1080497029, 0.3940588057, 0.1184834123, 0],
    "grepafloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "griseoviridin": ["streptogramin A antibiotic", 0, 0.2424977266, 0, 0],
    "hygromycin B": ["aminoglycoside", 0.2701242572, 3.1221582298, 2.1327014218, 0],
    "isepamicin": ["aminoglycoside", 35.2242031334, 7.1536829342, 99.1706161137, 0.218579235],
    "isoniazid": ["miscellaneous antibiotic", 0, 0, 0, 0],
    "kanamycin A": ["aminoglycoside", 35.2242031334, 7.1536829342, 99.1706161137, 0.218579235],
    "kasugamicin": ["aminoglycoside", 0, 0, 0, 0],
    "kirromycin-like antibiotics": ["elfamycin", 0, 0, 0, 0],
    "levofloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "lincomycin": ["lincosamide", 0.0540248514, 0.2424977266, 0, 0.218579235],
    "lincosamide": ["antibiotic molecule", 0.1080497029, 0.4546832373, 0, 0.218579235],
    "linezolid": ["oxazolidinone antibiotic", 0, 0, 0, 0],
    "lipopeptide antibiotic": ["antibiotic molecule", 66.5586169638, 98.9390724462, 99.2298578199, 0.5464480874],
    "lividomycin": ["aminoglycoside", 0, 0, 0, 0],
    "lividomycin A": ["lividomycin", 1.4046461372, 5.7290087905, 2.7251184834, 0.1092896175],
    "lividomycin B": ["lividomycin", 1.4046461372, 5.7290087905, 2.7251184834, 0.1092896175],
    "lomefloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "macrolide": ["antibiotic molecule", 30.037817396, 7.9418005456, 0.1184834123, 97.3770491803],
    "madumycin II": ["streptogramin A antibiotic", 0, 0.2424977266, 0, 0],
    "mafenide": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "methymycin": ["macrolide", 0, 0, 0, 0],
    "minocycline": ["tetracycline derivative", 1.8368449487, 0.4546832373, 0, 1.6393442623],
    "miscellaneous antibiotic": ["antibiotic molecule", 0, 0, 0, 0],
    "moxifloxacin": ["fluoroquinolone", 95.2998379254, 99.9696877842, 99.8815165877, 98.1420765027],
    "mupirocin": ["miscellaneous antibiotic", 0, 0, 0, 0],
    "nalidixic acid": ["fluoroquinolone", 65.6401944895, 99.9696877842, 100, 0.5464480874],
    "narbomycin": ["macrolide", 0, 0, 0, 0],
    "neomycin": ["aminoglycoside", 35.2242031334, 6.9414974235, 99.1706161137, 0.218579235],
    "netilmicin": ["aminoglycoside", 34.3057806591, 4.7287056684, 99.1706161137, 0.1092896175],
    "norfloxacin": ["fluoroquinolone", 95.4078876283, 99.9696877842, 99.9407582938, 98.1420765027],
    "novobiocin": ["aminocoumarin antibiotic", 63.6952998379, 99.9090633525, 99.8815165877, 0.5464480874],
    "nucleoside antibiotic": ["antibiotic molecule", 0, 0, 0, 0],
    "ofloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "oleandomycin": ["macrolide", 0.1080497029, 7.6386783874, 0, 0.1092896175],
    "ostreogrycin B3": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "oxacillin": ["penam", 60.1836844949, 99.8484389209, 99.3483412322, 0.5464480874],
    "oxytetracycline": ["tetracycline derivative", 1.8368449487, 0.4546832373, 0, 1.6393442623],
    "paromomycin": ["aminoglycoside", 1.4046461372, 5.7290087905, 2.7251184834, 0.1092896175],
    "patricin A": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "patricin B": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "pefloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "peptide antibiotic": ["antibiotic molecule", 61.2641815235, 99.8787511367, 99.8222748815, 0.5464480874],
    "phenicol": ["antibiotic molecule", 1.6747703944, 4.455895726, 1.5402843602, 0.1092896175],
    "pikromycin": ["macrolide", 0, 0, 0, 0],
    "polyamine antibiotic": ["antibiotic molecule", 0.1080497029, 0, 0, 0],
    "polymyxin": ["lipopeptide antibiotic", 24.14910859, 99.6362534101, 0, 0.4371584699],
    "pristinamycin IA": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "pristinamycin IB": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "pristinamycin IIA": ["streptogramin A antibiotic", 0, 0.2424977266, 0, 0],
    "pulvomycin": ["elfamycin", 0, 0, 0, 0],
    "puromycin": ["aminonucleoside antibiotic", 0, 0, 0, 0],
    "pyrazinamide": ["miscellaneous antibiotic", 0, 0, 0, 0],
    "quinupristin": ["streptogramin B antibiotic", 29.6056185845, 0.2424977266, 0, 97.3770491803],
    "ribostamycin": ["aminoglycoside", 1.4046461372, 5.7290087905, 2.7251184834, 0.1092896175],
    "rifabutin": ["rifamycin antibiotic", 66.2344678552, 98.9390724462, 99.2298578199, 0.5464480874],
    "rifampin": ["rifamycin antibiotic", 66.2344678552, 98.9390724462, 99.2298578199, 0.5464480874],
    "rifamycin antibiotic": ["antibiotic molecule", 66.5586169638, 98.9390724462, 99.2298578199, 0.5464480874],
    "rifapentine": ["rifamycin antibiotic", 66.2344678552, 98.9390724462, 99.2298578199, 0.5464480874],
    "rifaximin": ["rifamycin antibiotic", 66.2344678552, 98.9390724462, 99.2298578199, 0.5464480874],
    "roxithromycin": ["macrolide", 0.3241491086, 7.8508638982, 0.1184834123, 0.1092896175],
    "sisomicin": ["aminoglycoside", 34.3057806591, 4.7287056684, 99.1706161137, 0.1092896175],
    "sparfloxacin": ["fluoroquinolone", 95.2998379254, 99.9696877842, 99.8815165877, 98.1420765027],
    "spectinomycin": ["aminoglycoside", 4.3219881145, 15.8836010912, 4.028436019, 0.1092896175],
    "spiramycin": ["macrolide", 0.1080497029, 0.5759321006, 0, 0.1092896175],
    "streptogramin antibiotic": ["antibiotic molecule", 29.8217179903, 0.3637465899, 0, 97.3770491803],
    "streptomycin": ["aminoglycoside", 10.5888708806, 33.1615641103, 8.2938388626, 0.218579235],
    "streptothricin": ["nucleoside antibiotic", 0.8643976229, 2.6068505608, 0, 0],
    "sulfacetamide": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfadiazine": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfadimidine": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfadoxine": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfamethizole": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfamethoxazole": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfasalazine": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfisoxazole": ["sulfonamide", 60.075634792, 99.8787511367, 99.3483412322, 0.5464480874],
    "sulfonamide": ["antibiotic molecule", 64.5596974608, 99.8787511367, 99.4668246445, 0.5464480874],
    "sulfones": ["antibiotic molecule", 64.5056726094, 99.8787511367, 99.4668246445, 0.5464480874],
    "teicoplanin": ["glycopeptide antibiotic", 0, 0, 0, 0],
    "telithromycin": ["macrolide", 0.1080497029, 7.6386783874, 0, 0.1092896175],
    "tetracycline": ["tetracycline derivative", 92.490545651, 99.8484389209, 99.3483412322, 97.3770491803],
    "tetracycline derivative": ["antibiotic molecule", 35.4943273906, 11.0639587754, 3.4360189573, 96.9398907104],
    "thiamphenicol": ["phenicol", 2.2150189087, 5.6683843589, 0.4739336493, 0.1092896175],
    "thiostrepton": ["peptide antibiotic", 0, 0, 0, 0],
    "tiamulin": ["pleuromutilin antibiotic", 0, 0, 0, 0],
    "tigecycline": ["glycylcycline", 30.1458670989, 0.0303122158, 0, 96.8306010929],
    "tobramycin": ["aminoglycoside", 34.3057806591, 4.7287056684, 99.1706161137, 0.1092896175],
    "trimethoprim": ["diaminopyrimidine", 2.4311183144, 20.885116702, 0.7109004739, 0.1092896175],
    "trovafloxacin": ["fluoroquinolone", 65.6401944895, 99.9696877842, 99.8815165877, 0.5464480874],
    "tunicamycin": ["mixture", 0, 0, 0, 0],
    "tylosin": ["macrolide", 0.1080497029, 0.5759321006, 0, 0.1092896175],
    "vernamycin B-gamma": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "vernamycin C": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0],
    "viomycin": ["tuberactinomycin", 0, 0, 0, 0],
    "virginiamycin S2": ["streptogramin B antibiotic", 0, 0.2424977266, 0, 0]
}
