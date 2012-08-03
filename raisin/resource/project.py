"""Project related resources"""

import random
import datetime
import urlparse

from raisin.resource.utils import register_resource
from raisin.resource.utils import get_dashboard_db
from raisin.resource.utils import get_experiment_dict
from raisin.resource.utils import escape_html

# http://genome-test.cse.ucsc.edu/ENCODE/otherTerms.html#sex
# XXX Needs to be verified
# B stands for both: a cell population with both male and female cells
# U stands for unknown
GENDER_MAPPING = {'B': 'male',
                  'F': 'female',
                  'M': 'male',
                  'U': 'male',
                  }


@register_resource(resolution="project", partition=False)
def info(dbs, confs):
    """Return the project info."""
    conf = confs['configurations'][0]
    description = [('Project Description', 'string'),
                   ('Species', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    sql = """
select proj_description,
       species
from projects
where project_id='%(projectid)s';
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    chart['table_data'] = rows
    return chart


@register_resource(resolution="project", partition=False)
def projects(dbs, confs):
    """Return the projects and their URLs."""
    description = [('Project Id', 'string'),
                   ('URL', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    results = []
    for projectid in dbs.keys():
        results.append((projectid, '/project/%s' % projectid))
    chart['table_data'] = results
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_technologies(dbs, confs):
    """Return the technologies for the RNA dashboard."""
    description = [('Id', 'string'),
                   ('Title', 'string'),
                   ('Description', 'string'),
                   ('URL', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description

    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)

    sql = """
select name,
       displayName,
       description,
       descriptionUrl
from technology"""
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    results = []
    for row in rows:
        results.append((row[0], row[1], escape_html(row[2]), row[3]))
    chart['table_data'] = results
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_rna_fractions(dbs, confs):
    """Return the RNA fractions for the RNA dashboard."""
    description = [('Id', 'string'),
                   ('Title', 'string'),
                   ('Description', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)

    sql = """
select ucscName,
       displayName,
       description
from rnaExtract"""
    cursor = dashboard_db.query(sql)

    rows = cursor.fetchall()
    cursor.close()
    chart['table_data'] = rows
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_localizations(dbs, confs):
    """Return the localizations for the RNA dashboard."""
    description = [('Id', 'string'),
                   ('Title', 'string'),
                   ('Description', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)

    sql = """
select ucscName,
       displayName,
       description
from localization"""
    cursor = dashboard_db.query(sql)

    rows = cursor.fetchall()
    cursor.close()
    chart['table_data'] = rows
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_files(dbs, confs):
    """Return the files for the RNA dashboard.

    See
        http://genome-test.cse.ucsc.edu/ENCODE/otherTerms.html
    and
        http://genome-test.cse.ucsc.edu/cgi-bin/hgEncodeVocab?type=%22typeOfTerm%22

    for more info about file attributes.
    """
    description = [('Url', 'string'),
                   ('Attributes', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)

    sql = """
select file.url,
       file.allAttributes
from
    file
"""
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    chart['table_data'] = rows
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard(dbs, confs):
    """Return RNA dashboard."""
    description = [('Sample Grant Name', 'string'),
                   ('Cell Type', 'string'),
                   ('Cell Type Id', 'string'),
                   ('Tier', 'number'),
                   ('Localization', 'string'),
                   ('Localization Id', 'string'),
                   ('RNA Extract', 'string'),
                   ('RNA Extract Id', 'string'),
                   ('Technology', 'string'),
                   ('Technology Id', 'string'),
                   ('File at UCSC', 'number'),
                   ('File Raw Type', 'number'),
                   ('Sample Replicate', 'number'),
                   ('Sample Id', 'string'),
                   ('Sample Internal Name', 'string'),
                   ('Replicate Lab', 'string'),
                   ('Replicate Read Type', 'string'),
                   ('Replicate Insert Length', 'string'),
                   ('Replicate Tech Replicate', 'number'),
                   ('Replicate Id', 'string'),
                   ('File Type', 'string'),
                   ('File View', 'string'),
                   ('File Lab', 'string'),
                   ('File URL', 'string'),
                   ('File Size', 'string'),
                   ('File View de novo', 'number'),
                   ('Restricted until', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)

    sql = """
SELECT sample.grantName,
       cell.displayName as cellName,
       sample.cell,
       cell.tier,
       localization.displayName as localization,
       localization.ucscName,
       rnaExtract.displayName as rnaExtract,
       rnaExtract.ucscName,
       technology.displayName as technology,
       technology.name,
       file.atUcsc,
       fileType.rawType,
       sample.replicate as bioRep,
       sample.id as sample,
       sample.internalName,
       experiment.lab as expLab,
       experiment.readType,
       experiment.insertLength,
       experiment.techReplicate as techRep,
       experiment.id as expId,
       file.fileType,
       fileView.displayName,
       file.lab as endLab,
       file.url,
       file.size,
       fileView.deNovo,
       file.dateSubmitted
FROM sample,
     technology,
     file,
     experiment,
     fileType,
     localization,
     rnaExtract,
     cell,
     fileView
WHERE
      fileType != 'BAI'
AND
      file.experiment_data_processing = experiment.id
AND
      file.fileType = fileType.name
AND
      file.fileView = fileView.name
AND
      experiment.sampleName = sample.id
AND
      experiment.technology = technology.name
AND
      sample.localization = localization.ucscName
AND
      sample.rnaExtract = rnaExtract.ucscName
AND
      sample.cell = cell.ucscName"""
    cursor = dashboard_db.query(sql)

    rows = cursor.fetchall()
    cursor.close()

    results = []
    for row in rows:
        result = list(row)
        if row[10] == 1:
            if not result[-1] is None:
                end = result[-1] + datetime.timedelta(9 * 365 / 12)
                if end > datetime.date.today():
                    result[-1] = "%s-%s-%s" % (end.year, end.month, end.day)
                else:
                    result[-1] = None
        else:
            result[-1] = 'To be decided'
        results.append(result)
    chart['table_data'] = results
    return chart


@register_resource(resolution="project", partition=False)
def project_downloads(dbs, confs):
    """Return the project downloads."""
    # pylint: disable-msg=W0613
    # The database is not needed for this static resource.
    projectid = confs['configurations'][0]['projectid']
    description = [('File Name', 'string'),
                   ('Feature', 'string'),
                   ('Measurement', 'string'),
                   ('.csv File Download Link', 'string'),
                   ]
    chart = {}
    chart['table_description'] = description
    stats = (("Gene",
              "Expression (RPKM)",
              "gene_expression_rpkm"),
             ("Transcript",
              "Expression (RPKM)",
              "transcript_expression_rpkm"),
             ("Exon",
              "Expression (RPKM)",
              "exon_expression_rpkm"),
             ("Exon",
              "Number of Reads",
              "exon_number_of_reads"),
             ("Novel Junctions from Annotated Exons",
              "Number of Reads",
              "novel_junctions_from_annotated_exons_number_of_reads"),
             ("Novel Junctions from Unannotated Exons",
              "Number of Reads",
              "novel_junctions_from_unannotated_exons_number_of_reads"))

    table = []
    downloads = dbs[projectid]['downloads']
    if downloads is None:
        # No downloads for this project, so just return a single empty line
        table.append(["", "", "", ""])
    else:
        url = urlparse.urljoin(downloads, "%s.csv.gz")
        filename = "%s.csv"
        for title, category, key in stats:
            table.append([filename % key, title, category, url % key])
    chart['table_data'] = table
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_results(dbs, confs):
    """Return the RNA dashboard results."""
    return _rnadashboard_results(dbs, confs)


def _rnadashboard_results(dbs, confs):
    """Return the RNA dashboard results."""
    if not confs['configurations'][0]['projectid'] == 'ENCODE':
        return None
    chart = {}
    description = _rnadashboard_results_description()
    description_keys = [d[0] for d in description]
    wheres = _rnadashboard_results_wheres(confs)
    rows = _rnadashboard_results_sql(dbs, confs, wheres)
    restricted = _rnadashboard_results_restricted(rows, description_keys)
    results = []
    for rest in restricted:
        results.append([rest[key] for key in description_keys])
    chart['table_description'] = description
    chart['table_data'] = results
    return chart


def _rnadashboard_results_description():
    """Return the RNA dashboard results descriptions."""
    description = []
    description.append(('Restricted until', 'string'))
    description.append(('File Type', 'string'))
    description.append(('File View', 'string'))
    description.append(('File Lab', 'string'))
    description.append(('File URL', 'string'))
    description.append(('File Size', 'string'))
    description.append(('File View de novo', 'number'))
    description.append(('Sample Grant Name', 'string'))
    description.append(('Cell Type', 'string'))
    description.append(('Cell Type Id', 'string'))
    description.append(('Tier', 'number'))
    description.append(('Localization', 'string'))
    description.append(('Localization Id', 'string'))
    description.append(('RNA Extract', 'string'))
    description.append(('RNA Extract Id', 'string'))
    description.append(('Technology', 'string'))
    description.append(('Technology Id', 'string'))
    description.append(('File at UCSC', 'number'))
    description.append(('File Raw Type', 'number'))
    description.append(('Sample Replicate', 'number'))
    description.append(('Sample Id', 'string'))
    description.append(('Sample Internal Name', 'string'))
    description.append(('Replicate Lab', 'string'))
    description.append(('Replicate Read Type', 'string'))
    description.append(('Replicate Insert Length', 'string'))
    description.append(('Replicate Tech Replicate', 'number'))
    description.append(('Replicate Id', 'string'))
    return description


def _rnadashboard_results_restricted(rows, description_keys):
    """Return the restricted RNA dashboard results."""
    results = []
    for row in rows:
        result = dict(zip(description_keys, row))
        if result['File at UCSC'] == 1:
            if not result['Restricted until'] is None:
                delta = datetime.timedelta(9 * 365 / 12)
                end = result['Restricted until'] + delta
                if end > datetime.date.today():
                    timestamp = "%s-%s-%s" % (end.year, end.month, end.day)
                    result['Restricted until'] = timestamp
                else:
                    result['Restricted until'] = None
        else:
            result['Restricted until'] = 'To be decided'
        results.append(result)
    return results


def _rnadashboard_results_wheres(confs):
    """Return the RNA dashboard where clause."""
    wheres = ""
    meta = get_experiment_dict(confs)
    if 'cell' in meta:
        wheres = wheres + """
AND
    sample.cell = '%(cell)s'""" % meta

    if 'localization' in meta:
        wheres = wheres + """
AND
    sample.localization = '%(localization)s'""" % meta

    if 'rnaExtract' in meta:
        wheres = wheres + """
AND
    sample.rnaExtract = '%(rnaExtract)s'""" % meta

    if 'lab' in meta:
        wheres = wheres + """
AND
    file.lab = '%(lab)s'""" % meta
    return wheres


def _rnadashboard_results_sql(dbs, confs, wheres=""):
    """Query the database for the RNA dashboard."""
    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)
    sql = """
SELECT file.dateSubmitted,
       file.fileType,
       fileView.displayName,
       file.lab as endLab,
       file.url,
       file.size,
       fileView.deNovo,
       sample.grantName,
       cell.displayName as cellName,
       sample.cell,
       cell.tier,
       localization.displayName as localization,
       localization.ucscName,
       rnaExtract.displayName as rnaExtract,
       rnaExtract.ucscName,
       technology.displayName as technology,
       technology.name,
       file.atUcsc,
       fileType.rawType,
       sample.replicate as bioRep,
       sample.id as sample,
       sample.internalName,
       experiment.lab as expLab,
       experiment.readType,
       experiment.insertLength,
       experiment.techReplicate as techRep,
       experiment.id as expId
FROM sample,
     technology,
     file,
     experiment,
     fileType,
     localization,
     rnaExtract,
     cell,
     fileView
WHERE
      fileType != 'BAI'
AND
      file.experiment_data_processing = experiment.id
AND
      file.fileType = fileType.name
AND
      file.fileView = fileView.name
AND
      experiment.sampleName = sample.id
AND
      experiment.technology = technology.name
AND
      sample.localization = localization.ucscName
AND
      sample.rnaExtract = rnaExtract.ucscName
AND
      sample.cell = cell.ucscName
%s""" % wheres
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


@register_resource(resolution=None, partition=False)
def rnadashboard_accessions(dbs, confs):
    """Produce accessions with information obtained from the RNA dashboard

    The accession file can be fetched like this to fetch all accessions for the lab CSHL

        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab/CSHL/rnadashboard/hg19/accessions

    Selecting rna type subsets can also be useful:

        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rnaExtract/CSHL-LONGPOLYA/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rnaExtract/CSHL-LONGNONPOLYA/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rnaExtract/CSHL-TOTAL/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rnaExtract/CSHL-SHORT/rnadashboard/hg19/accessions

        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rnaExtract/CALTECH-LONGPOLYA/rnadashboard/hg19/accessions


        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/rnaExtract/TOTAL/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/rnaExtract/SHORT/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/rnaExtract/LONGPOLYA/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/rnaExtract/LONGNONPOLYA/rnadashboard/hg19/accessions

    This is an example of an accession:

        [ExampleReplicateId]
        file_location = http://www.example.com/download/file-1-1.fastq.gz
                        http://www.example.com/download/file-2-1.fastq.gz
                        http://www.example.com/download/file-2-2.fastq.gz
                        http://www.example.com/download/file-2-1.fastq.gz
        species = Homo sapiens
        readType = 1x36
        rnaExtract = SHORTTOTAL
        localization = CYTOSOL
        replicate = 1
        qualities = solexa
        mate_id = Example_Rep1_2
                  Example_Rep2_1
                  Example_Rep2_2
                  Example_Rep1_1
        pair_id = Example_Rep1
                  Example_Rep2
                  Example_Rep2
                  Example_Rep1
        label = Bio1
                Bio2
                Bio2
                Bio1
    """
    return _rnadashboard_accessions(dbs, confs)


def _rnadashboard_accessions(dbs, confs):
    """Return the RNA dashboard accessions."""
    chart = {}

    description = []
    description.append(('accession', 'string'))
    description.append(('gender', 'string'))
    description.append(('file_location', 'string'))
    description.append(('readType', 'string'))
    description.append(('rnaExtract', 'string'))
    description.append(('localization', 'string'))
    description.append(('replicate', 'number'))
    description.append(('cell', 'string'))
    description.append(('species', 'string'))
    description.append(('qualities', 'string'))
    description.append(('file_type', 'string'))

    chart['table_description'] = description

    wheres = _rnadashboard_results_wheres(confs)
    fastqs = _fastqs(dbs, confs, wheres)

    accession_fastqs = {}

    for fastq in fastqs:
        accession_id = fastq["file.lab as endLab"]
        if not fastq["sample.internalName"] is None:
            internal = fastq["sample.internalName"]
            internal = internal.replace('-', 'Minus').replace('+', 'Plus')
            accession_id = accession_id + internal
        else:
            # Use an attribute value common between the files as a seed
            # for the accession id
            random.seed(fastq["file.experiment_data_processing"])
            # Create a random accession id in absence of an internal name
            accession_id = accession_id + str(random.random())[2:]
        if accession_id is None:
            # Use the file url as a seed for the accession id
            #random.seed(fastq["file.url"])
            # Create a random accession id in absence of an internal name
            #accession_id = str(random.random())[2:]
            accession_id = fastq["file.experiment_data_processing"]
        if accession_id in accession_fastqs:
            accession_fastqs[accession_id].append(fastq)
        else:
            accession_fastqs[accession_id] = [fastq]

    accession_list = accession_fastqs.keys()
    accession_list.sort()

    results = []
    for accession in accession_list:
        fastqs = accession_fastqs[accession]
        if len(fastqs) > 2:
            raise AttributeError
        for fastq in fastqs:
            attributes = _parse_all_attributes(fastq["file.allAttributes"])
            read_type = fastq["experiment.readType"]
            if read_type is None:
                read_type = attributes.get('readType', None)
            results.append((accession,
                            fastq["cell.sex"],
                            fastq["file.url"],
                            read_type,
                            fastq["rnaExtract.ucscName"],
                            fastq["localization.ucscName"],
                            fastq["sample.replicate as bioRep"],
                            fastq["sample.cell"],
                            "Homo sapiens",
                            "phred",
                            fastq["fileView.name"],
                            ))

    chart['table_data'] = results

    return chart


def _parse_all_attributes(all_attributes):
    """Parse the attributes in the RNA dashboard lines."""
    result = {}
    for attribute in all_attributes.split(';'):
        key, value = attribute.split('=')
        result[key.strip()] = value.strip()
    return result


def _fastqs(dbs, confs, wheres=""):
    """Return the fastq files only."""
    hgversion = confs['configurations'][0]['hgversion']
    dashboard_db = get_dashboard_db(dbs, hgversion)
    selects = ["file.dateSubmitted",
               "file.experiment_data_processing",
               "file.fileType",
               "fileView.displayName",
               "fileView.name",
               "file.lab as endLab",  # Produced the file using their pipeline
               "file.url",
               "file.size",
               "fileView.deNovo",
               "sample.grantName",
               "cell.displayName as cellName",
               "sample.cell",
               "cell.tier",
               "cell.sex",
               "localization.displayName as localization",
               "localization.ucscName",
               "rnaExtract.displayName as rnaExtract",
               "rnaExtract.ucscName",
               "technology.displayName as technology",
               "technology.name",
               "file.atUcsc",
               "fileType.rawType",
               "sample.replicate as bioRep",
               "sample.id as sample",
               "sample.internalName",
               "experiment.lab as expLab",     # Did the physical experiment
               "experiment.readType",
               "experiment.insertLength",
               "experiment.techReplicate as techRep",
               "experiment.id as expId",
               "file.allAttributes"]
    sql = """
SELECT %s
FROM sample,
     technology,
     file,
     experiment,
     fileType,
     localization,
     rnaExtract,
     cell,
     fileView
WHERE
      fileType != 'BAI'
AND
      file.experiment_data_processing = experiment.id
AND
      file.fileType = fileType.name
AND
      file.fileView = fileView.name
AND
      experiment.sampleName = sample.id
AND
      experiment.technology = technology.name
AND
      sample.localization = localization.ucscName
AND
      sample.rnaExtract = rnaExtract.ucscName
AND
      sample.cell = cell.ucscName
AND
      file.fileType = "FASTQ"
AND
      technology.name = "RNASEQ"
%s""" % (",".join(selects), wheres)
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result = []
    for row in rows:
        result.append(dict(zip(selects, row)))
    return result


@register_resource(resolution=None, partition=False)
def rnadashboard_replicates(dbs, confs):
    """Produce replicates with information obtained from the RNA dashboard

    The replicates file can be fetched like this to fetch replicates accessions for the lab CSHL

        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab/CSHL/rnadashboard/hg19/accessions
    """
    table = _rnadashboard_accessions(dbs, confs)

    chart = {}

    description = []
    description.append(('run', 'string'))
    description.append(('recipe', 'string'))
    description.append(('update-script', 'string'))
    description.append(('install-script', 'string'))
    description.append(('pipeline', 'string'))
    description.append(('accession', 'string'))

    chart['table_description'] = description

    seen_replicates = []

    result = []
    for accession in table['table_data']:
        if not accession[0] in seen_replicates:
            result.append((accession[0],
                           "z3c.recipe.runscript",
                           "prepare.py:main",
                           "prepare.py:main",
                           GENDER_MAPPING[accession[1]],
                           accession[0])
                          )
        seen_replicates.append(accession[0])

    chart['table_data'] = result
    return chart


def rnadashboard_results_pending(dbs, confs):
    """Return the RNA dashboard replicates pending in the pipeline."""
    description = _rnadashboard_results_description()
    description_keys = [d[0] for d in description]
    wheres = _rnadashboard_results_wheres(confs)
    rows = _rnadashboard_results_sql(dbs, confs, wheres)
    restricted = _rnadashboard_results_restricted(rows, description_keys)
    results = {}
    for rest in restricted:
        if rest['File Type'] != 'FASTQ':
            continue
        if rest['Technology Id'] != 'RNASEQ':
            continue
        key = []
        key.append(rest['Replicate Lab'])
        key.append(rest['Cell Type Id'])
        key.append(rest['Localization Id'])
        key.append(rest['RNA Extract Id'])
        key.append('GENCODEv3c')
        read_type = rest['Replicate Read Type']
        if read_type is None:
            read_length = None
            paired = None
        else:
            if 'x' in read_type:
                read_length = read_type.split('x')[1]
                # 2 -> 1 Paired is true
                # 1 -> 0 Paired is false
                paired = str(int(read_type.split('x')[0]) - 1)
            if 'D' in read_length:
                read_length = read_length.split('D')[0]
            if not read_length.isdigit():
                # read_length needs to be a number, otherwise don't pass it on
                raise AttributeError
            key.append(read_length)
            key.append(paired)
        rest['Read Length'] = read_length
        rest['Paired'] = paired
        results['-'.join(key)] = rest
    return results
