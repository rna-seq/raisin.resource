"""Project related resources"""

import datetime
from raisin.resource.utils import register_resource
from raisin.resource.utils import get_dashboard_db
from raisin.resource.utils import get_experiment_dict


@register_resource(resolution="project", partition=False)
def info(dbs, confs):
    conf = confs['configurations'][0]
    chart = {}
    chart['table_description'] = [('Project Description', 'string'),
                                  ('Species',             'string'),
                                 ]
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
    chart = {}
    chart['table_description'] = [('Project Id', 'string'),
                                  ('URL',        'string'),
                                 ]
    results = []
    for projectid in dbs.keys():
        results.append((projectid, '/project/%s/tab/experiments/' % projectid))
    chart['table_data'] = results
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_technologies(dbs, confs):
    chart = {}
    chart['table_description'] = [('Id',          'string'),
                                  ('Title',       'string'),
                                  ('Description', 'string'),
                                  ('URL',         'string'),
                                 ]

    dashboard_db = get_dashboard_db(dbs, confs['configurations'][0]['hgversion'])

    sql = """
select name,
       displayName,
       description,
       descriptionUrl
from technology"""
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()

    def escape(html):
        """Returns the given HTML with ampersands, quotes and carets encoded."""
        return html.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#39;')

    def unescape(html):
        """Returns the given original HTML decoding ampersands, quotes and carets."""
        return html.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace("&#39;", "'")

    results = []
    for row in rows:
        results.append((row[0], row[1], escape(row[2]), row[3]))
    chart['table_data'] = results
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_rna_fractions(dbs, confs):
    chart = {}
    chart['table_description'] = [('Id',           'string'),
                                  ('Title',        'string'),
                                  ('Description',  'string'),
                                 ]

    dashboard_db = get_dashboard_db(dbs, confs['configurations'][0]['hgversion'])

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
def rnadashboard_compartments(dbs, confs):
    chart = {}
    chart['table_description'] = [('Id',           'string'),
                                  ('Title',        'string'),
                                  ('Description',  'string'),
                                 ]

    dashboard_db = get_dashboard_db(dbs, confs['configurations'][0]['hgversion'])

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
    """
    See
        http://genome-test.cse.ucsc.edu/ENCODE/otherTerms.html
    and
        http://genome-test.cse.ucsc.edu/cgi-bin/hgEncodeVocab?type=%22typeOfTerm%22

    for more info about file attributes.
    """
    chart = {}
    chart['table_description'] = [('Url',        'string'),
                                  ('Attributes', 'string'),
                                 ]

    dashboard_db = get_dashboard_db(dbs, confs['configurations'][0]['hgversion'])

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
    chart = {}
    chart['table_description'] = [('Sample Grant Name',         'string'),
                                  ('Cell Type',                 'string'),
                                  ('Cell Type Id',              'string'),
                                  ('Tier',                      'number'),
                                  ('Localization',              'string'),
                                  ('Localization Id',           'string'),
                                  ('RNA Extract',               'string'),
                                  ('RNA Extract Id',            'string'),
                                  ('Technology',                'string'),
                                  ('Technology Id',             'string'),
                                  ('File at UCSC',              'number'),
                                  ('File Raw Type',             'number'),
                                  ('Sample Replicate',          'number'),
                                  ('Sample Id',                 'string'),
                                  ('Sample Internal Name',      'string'),
                                  ('Experiment Lab',            'string'),
                                  ('Experiment Read Type',      'string'),
                                  ('Experiment Insert Length',  'string'),
                                  ('Experiment Tech Replicate', 'number'),
                                  ('Experiment Id',             'string'),
                                  ('File Type',                 'string'),
                                  ('File View',                 'string'),
                                  ('File Lab',                  'string'),
                                  ('File URL',                  'string'),
                                  ('File Size',                 'string'),
                                  ('File View de novo',         'number'),
                                  ('Restricted until',          'string'),
                                 ]

    dashboard_db = get_dashboard_db(dbs, confs['configurations'][0]['hgversion'])

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
    projectid = confs['configurations'][0]['projectid']
    chart = {}
    chart['table_description'] = [('File Name',               'string'),
                                  ('Feature',                 'string'),
                                  ('Measurement',             'string'),
                                  ('.csv File Download Link', 'string'),
                                 ]

    stats = (("Gene", "Expression (RPKM)", "gene_expression_rpkm"),
             ("Transcript", "Expression (RPKM)", "transcript_expression_rpkm"),
             ("Exon", "Expression (RPKM)", "exon_expression_rpkm"),
             ("Exon", "Number of Reads", "exon_number_of_reads"),
             ("Novel Junctions from Annotated Exons", "Number of Reads", "novel_junctions_from_annotated_exons_number_of_reads"),
             ("Novel Junctions from Unannotated Exons", "Number of Reads", "novel_junctions_from_unannotated_exons_number_of_reads"))

    ftp = "http://genome.crg.es/~mroder/rnaseq/%s/%s.csv.gz"
    filename = "%s.csv"

    table = []
    for title, category, key in stats:
        table.append([filename % key, title, category, ftp % (projectid, key)])

    chart['table_data'] = table
    return chart


@register_resource(resolution=None, partition=False)
def rnadashboard_results(dbs, confs):
    chart = {}
    description = []
    description.append(('Restricted until',          'string'))
    description.append(('File Type',                 'string'))
    description.append(('File View',                 'string'))
    description.append(('File Lab',                  'string'))
    description.append(('File URL',                  'string'))
    description.append(('File Size',                 'string'))
    description.append(('File View de novo',         'number'))
    description.append(('Sample Grant Name',         'string'))
    description.append(('Cell Type',                 'string'))
    description.append(('Cell Type Id',              'string'))
    description.append(('Tier',                      'number'))
    description.append(('Localization',              'string'))
    description.append(('Localization Id',           'string'))
    description.append(('RNA Extract',               'string'))
    description.append(('RNA Extract Id',            'string'))
    description.append(('Technology',                'string'))
    description.append(('Technology Id',             'string'))
    description.append(('File at UCSC',              'number'))
    description.append(('File Raw Type',             'number'))
    description.append(('Sample Replicate',          'number'))
    description.append(('Sample Id',                 'string'))
    description.append(('Sample Internal Name',      'string'))
    description.append(('Experiment Lab',            'string'))
    description.append(('Experiment Read Type',      'string'))
    description.append(('Experiment Insert Length',  'string'))
    description.append(('Experiment Tech Replicate', 'number'))
    description.append(('Experiment Id',             'string'))

    chart['table_description'] = description

    if not confs['configurations'][0]['projectid'] == 'ENCODE':
        return None

    wheres = _rnadashboard_results_wheres(confs)
    rows = _rnadashboard_results(dbs, confs, wheres)

    results = []
    for row in rows:
        result = list(row)
        if row[17] == 1:
            if not result[0] is None:
                end = result[0] + datetime.timedelta(9 * 365 / 12)
                if end > datetime.date.today():
                    result[0] = "%s-%s-%s" % (end.year, end.month, end.day)
                else:
                    result[0] = None
        else:
            result[0] = 'To be decided'
        results.append(result)
    chart['table_data'] = results
    return chart


def _rnadashboard_results_wheres(confs):
    wheres = ""
    meta = get_experiment_dict(confs)
    if 'cell_type' in meta:
        wheres = wheres + """
AND
    sample.cell = '%(cell_type)s'""" % meta

    if 'compartment' in meta:
        wheres = wheres + """
AND
    sample.localization = '%(compartment)s'""" % meta

    if 'rna_type' in meta:
        wheres = wheres + """
AND
    sample.rnaExtract = '%(rna_type)s'""" % meta

    if 'lab' in meta:
        wheres = wheres + """
AND
    file.lab = '%(lab)s'""" % meta
    return wheres


def _rnadashboard_results(dbs, confs, wheres="", additional_selects=""):
    dashboard_db = get_dashboard_db(dbs, confs['configurations'][0]['hgversion'])
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
       experiment.id as expId%s
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
%s""" % (additional_selects, wheres)
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

        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rna_type/CSHL-LONGPOLYA/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rna_type/CSHL-LONGNONPOLYA/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rna_type/CSHL-TOTAL/rnadashboard/hg19/accessions
        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/lab-rna_type/CSHL-SHORT/rnadashboard/hg19/accessions

    This is an example of an accession:

        [ExampleRunId]
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
    chart = {}

    description = []
    description.append(('accession',     'string'))
    description.append(('file_location', 'string'))
    #description.append(('species',       'string'))
    description.append(('readType',      'string'))
    description.append(('rnaExtract',    'string'))
    description.append(('localization',  'string'))
    description.append(('replicate',     'number'))
    #description.append(('qualities',     'string'))
    #description.append(('mate_id',       'string'))
    #description.append(('pair_id',       'string'))
    #description.append(('label',         'string'))

    chart['table_description'] = description

    additional_selects = """,
    file.allAttributes
"""
    wheres = _rnadashboard_results_wheres(confs)
    rows = _rnadashboard_results(dbs, confs, wheres, additional_selects)

    # Only create accessions for fastq files
    rows = [row for row in rows if row[1] in ['FASTQ', 'FASTQRD1', 'FASTQRD2']]

    accession_files = {}

    for row in rows:
        accession = row[21]
        if id in accession_files:
            accession_files[accession].append(row)
        else:
            accession_files[accession] = [row]

    accession_list = accession_files.keys()
    accession_list.sort()

    results = []
    for accession in accession_list:
        files = accession_files[accession]
        if len(files) > 2:
            raise AttributeError
        for file in files:
            all_attributes = _parse_all_attributes(file[27])
            readType = file[23]
            if readType is None:
                readType = all_attributes.get('readType', None)
            results.append((accession,
                            file[4],
                            readType,
                            file[14],
                            file[12],
                            file[19],
                            ))

    chart['table_data'] = results

    return chart


def _parse_all_attributes(all_attributes):
    result = {}
    for attribute in all_attributes.split(';'):
        key, value = attribute.split('=')
        result[key.strip()] = value.strip()
    return result
