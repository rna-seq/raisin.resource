from raisin.box.boxes import get_lines
from raisin.box.config import PICKLED    
from raisin.resource.utils import register_resource
from raisin.resource.utils import get_dashboard_db

@register_resource(resolution="project", partition=False)        
def info(dbs, confs):
    conf = confs['configurations'][0]
    chart = {}
    chart['table_description'] = [('Project Description', 'string'),
                                  ('Species',             'string'),
                                 ]
    info = {}
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
        results.append( (projectid, '/project/%s/' % projectid) )
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
        results.append( (row[0], row[1], escape(row[2]), row[3]) )
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
    chart['table_description'] = [('Sample Grant Name',        'string'),
                                  ('Cell Type',                'string'), 
                                  ('Cell Type Id',             'string'), 
                                  ('Tier',                     'number'),
                                  ('Localization',             'string'),
                                  ('Localization Id',          'string'),
                                  ('RNA Extract',              'string'), 
                                  ('RNA Extract Id',           'string'),
                                  ('Technology',               'string'),
                                  ('Technology Id',            'string'),
                                  ('File at UCSC',             'number'),
                                  ('File Raw Type',            'number'),
                                  ('Sample Replicate',         'number'),
                                  ('Sample Id',                'string'),
                                  ('Sample Internal Name',     'string'),
                                  ('Experiment Lab',           'string'),
                                  ('Experiment Read Type',     'string'),
                                  ('Experiment Insert Length', 'string'),
                                  ('Experiment Tech Replicate','number'),
                                  ('Experiment Id',            'string'),
                                  ('File Type',                'string'),
                                  ('File View',                'string'),
                                  ('File Lab',                 'string'),
                                  ('File URL',                 'string'),
                                  ('File Size',                'string'),
                                  ('File View de novo',        'number'),
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
       file.fileView,
       file.lab as endLab,
       file.url, 
       file.size,
       fileView.deNovo
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
    chart['table_data'] = rows
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
    for title, category, id in stats:
        table.append( [filename % id, title, category, ftp % (projectid, id)] )
    
    chart['table_data'] = table
    return chart
        