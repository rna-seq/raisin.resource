from utils import run
from utils import get_rna_type_display_mapping
from utils import get_cell_type_display_mapping
from utils import get_compartment_display_mapping
from utils import get_experiment_chart
from utils import get_experiment_id
from utils import get_experiment_dict
from utils import get_experiment_result
from utils import get_experiment_order_by
from utils import get_experiment_labels
from utils import get_experiment_where
from utils import register_resource

@register_resource(resolution=None, partition=False)      
def experiment_info(dbs, confs):
    conf = confs['configurations'][0]
    experimentid = conf['experimentid']
    chart = {}
    chart['table_description'] = [('Read Length',        'number'),
                                  ('Mismatches',         'number'),
                                  ('Description',        'string'),
                                  ('Date',               'string'),
                                  ('Cell Type',          'string'),
                                  ('RNA Type',           'string'),
                                  ('Compartment',        'string'),
                                  ('Bio Replicate',      'string'),
                                  ('Partition',          'string'),
                                  ('Species',            'string'),
                                  ('Annotation Version', 'string'),
                                  ('Annotation Source',  'string'),
                                  ('Genome Assembly',    'string'),
                                  ('Genome Source',      'string'),
                                  ('Genome Gender',      'string'),
                                  ('UCSC Custom Track',  'string'),
                                 ]

    conf = confs['configurations'][0]

    meta = get_experiment_dict(confs)

    result = []

    sql = """
select experiment_id,
       project_id,
       species_id,
       genome_id,
       annotation_id,
       template_file,
       read_length,
       mismatches,
       exp_description,
       expDate,
       CellType,
       RNAType,
       Compartment,
       Bioreplicate,
       partition
from experiments 
%s""" % get_experiment_where(confs, meta)
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        chart['table_data'] = [[None] * len(chart['table_description'])]
        return chart

    species_id = rows[0][2]
    genome_id = rows[0][3]
    annotation_id = rows[0][4]        
    result.append(int(rows[0][6]))
    result.append(int(rows[0][7]))
    result.append(rows[0][8])
    result.append(str(rows[0][9]))
    # Use labels instead of the raw values
    result.append(get_cell_type_display_mapping(dbs).get(rows[0][10], rows[0][10]))
    result.append(get_rna_type_display_mapping(dbs).get(rows[0][11], rows[0][11]))
    result.append(get_compartment_display_mapping(dbs).get(rows[0][12], rows[0][12]))
    result.append(rows[0][13])
    result.append(rows[0][14])
    
    sql = """
select species_id,
       species,
       genus,
       sp_alias,
       abbreviation
from species_info 
where species_id='%s'
""" % species_id
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result.append(rows[0][1])
    

    sql = """
select annotation_id, 
       species_id,
       annotation,
       location,
       version,
       source
from annotation_files where annotation_id='%s'
""" % annotation_id
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result.append(rows[0][4])
    result.append(rows[0][5])

    sql = """
select genome_id,
       species_id,
       genome,
       location,
       assembly,
       source,
       gender
from genome_files where genome_id='%s'
""" % genome_id
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result.append(rows[0][4])
    result.append(rows[0][5])
    result.append(rows[0][6])
    if experimentid == 'Ging001N':
        result.append("http://genome.ucsc.edu/cgi-bin/hgTracks?org=human&hgct_customText=ftp://ftp.encode.crg.cat/pub/rnaseq/encode/001N/BAM/001N.merged.track.txt")
    else:
        result.append("")        
    chart['table_data'] = [result,]
    return chart

@register_resource(resolution=None, partition=False)
def experiments(dbs, confs):
    """
    Used for generating the buildout.cfg for pipeline.buildout.
    """
    chart = {}
    chart['table_description'] = [('Project id',               'string'),
                                  ('Experiment id',            'string'),
                                  ('Species',                  'string'),
                                  ('Genome file name',         'string'),
                                  ('Genome file location',     'string'),
                                  ('Genome assembly',          'string'),
                                  ('Genome gender',            'string'),
                                  ('Annotation file name',     'string'),
                                  ('Annotation file location', 'string'),
                                  ('Annotation version',       'string'),
                                  ('Template file',            'string'), 
                                  ('Mismatches',               'number'), 
                                  ('Description',              'string'),
                                 ]
                                 
    results = []
    for projectid in dbs.keys():
        rows, failed = run(dbs, _experiments, {'projectid':projectid})
        if not failed:
            results = results + list(rows)

    chart['table_data'] = results
    return chart

def _experiments(dbs, conf):
    # Only return the experiment infos if this is an official project
    sql = """
select project_id,
       experiment_id,
       species_info.species,
       genome_files.genome,
       genome_files.location,
       genome_files.assembly,
       genome_files.gender,
       annotation_files.annotation,
       annotation_files.location,
       annotation_files.version,
       template_file,
       mismatches,
       exp_description
from experiments,
     species_info,
     genome_files,
     annotation_files
where 
      project_id = '%(projectid)s'
and
      experiments.species_id = species_info.species_id
and
      experiments.genome_id = genome_files.genome_id
and 
      experiments.annotation_id = annotation_files.annotation_id
order by 
     experiment_id;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
    
@register_resource(resolution=None, partition=False)
def experiments_configurations(dbs, confs):
    """
    The experiments (read: pipeline runs) have a number of configuration parameters that define
    them uniquely.
    
    project_id:    Defines what project this experiment was made for
    experiment_id: Unique identifier of the experiment
    read_length:   Experiments in a project can have different read lengths
    CellType:      Experiments may come from different cell types
    RNAType:       Experiments may have been done with different rna types
    Compartment:   Experiments may have been prepared from different cell compartments
    Bioreplicate:  Experiments are done for a bio replicate
    partition:     Experiments can be done for samples coming from different conditions
    """
    chart = {}
    chart['table_description'] = [('Project id',               'string'),
                                  ('Experiment id',            'string'),
                                  ('Read Length',              'number'),
                                  ('Cell Type',                'string'),
                                  ('RNA Type',                 'string'),
                                  ('Compartment',              'string'),
                                  ('Bio Replicate',            'string'),
                                  ('Partition',                'string'),
                                 ]

    results = []
    for projectid in dbs.keys():
        rows, failed = run(dbs, _experiments_configurations, {'projectid':projectid})
        if not failed:
            results = results + list(rows)

    chart['table_data'] = results
    return chart

def _experiments_configurations(dbs, conf):
    sql = """
select project_id,
       experiment_id,
       read_length,
       CellType,
       RNAType,
       Compartment,
       Bioreplicate,
       partition
from experiments;""" 
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

@register_resource(resolution='project', partition=False)
def project_experiments(dbs, confs):
    conf = confs['configurations'][0]
    projectid = conf['projectid']

    chart = {}
    chart['table_description'] = [('Experiment Id',            'string'),
                                  ('Project Id',               'string'),
                                  ('Species',                  'string'),
                                  ('Genome file name',         'string'),
                                  ('Genome file location',     'string'),
                                  ('Genome assembly',          'string'),
                                  ('Genome gender',            'string'),
                                  ('Annotation file name',     'string'),
                                  ('Annotation file location', 'string'),
                                  ('Annotation version',       'string'),
                                  ('Template File',            'string'),
                                  ('Read Length',              'number'),
                                  ('Mismatches',               'number'),
                                  ('Experiment Description',   'string'),
                                  ('Experiment Date',          'string'),
                                  ('Cell Type',                'string'),
                                  ('RNA Type',                 'string'),
                                  ('Compartment',              'string'),
                                  ('Bioreplicate',             'string'),
                                  ('Partition',                'string'),
                                  ('URL',                      'string'),
                                  ('Annotation Version',       'string'),
                                  ('Lab',                      'string'),
                                 ]
            
    sql = """
select project_id,
       experiment_id,
       species_info.species,
       genome_files.genome,
       genome_files.location,
       genome_files.assembly,
       genome_files.gender,
       annotation_files.annotation,
       annotation_files.location,
       annotation_files.version,
       template_file,
       read_length,
       mismatches,
       exp_description,
       expDate,
       CellType,
       RNAType,
       Compartment,
       Bioreplicate,
       partition,
       annotation_version,
       lab
from experiments,
     species_info,
     genome_files,
     annotation_files
where 
      project_id='%s'
and
      experiments.species_id = species_info.species_id
and
      experiments.genome_id = genome_files.genome_id
and 
      experiments.annotation_id = annotation_files.annotation_id;
""" % projectid
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    results = []
    for row in rows:
        # Augment the information from the database with a url and a text
        row = list(row)
        experimentid = get_experiment_id(confs, 
                                         {'projectid':row[1],
                                          'cell_type':row[15],
                                          'rna_type':row[16],
                                          'compartment':row[17],
                                          'bio_replicate':row[18],
                                          'partition':row[19],
                                          'annotation_version':row[20],
                                          'lab':row[21],
                                          } )
        row.append('/project/%s/experiment/%s/statistics/overview' % (row[0], experimentid) )            
        results.append(row)
    chart['table_data'] = results
    return chart

@register_resource(resolution='project', partition=False)    
def project_experimentstableraw(dbs, confs):
    return _project_experimentstable(dbs, confs, raw=True)
    
@register_resource(resolution='project', partition=False)    
def project_experimentstable(dbs, confs):
    return _project_experimentstable(dbs, confs, raw=False)

def _project_experimentstable(dbs, confs, raw=True):
    chart = get_experiment_chart(confs)
    conf = confs['configurations'][0]
    # Only return the experiment infos if this is an official project
    sql = """
select experiment_id,
       species_info.species,
       genome_files.genome,
       genome_files.location,
       genome_files.assembly,
       genome_files.gender,
       annotation_files.annotation,
       annotation_files.location,
       annotation_files.version,
       template_file,
       read_length,
       mismatches,
       exp_description,
       expDate,
       CellType,
       RNAType,
       Compartment,
       Bioreplicate,
       partition,
       annotation_version,
       lab
from experiments,
     species_info,
     genome_files,
     annotation_files
where 
      experiments.species_id = species_info.species_id
and
      experiments.genome_id = genome_files.genome_id
and 
      experiments.annotation_id = annotation_files.annotation_id
and   
      project_id = '%s'
%s""" % (conf['projectid'], get_experiment_order_by(confs))
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    experimentids = {}

    rna_types = get_rna_type_display_mapping(dbs)
    cell_types = get_cell_type_display_mapping(dbs)
    compartments = get_compartment_display_mapping(dbs)
        
    for row in rows:
        meta = {}
        meta['projectid'] = conf['projectid']
        meta['experimentid'] = row[0]
        meta['cell_type'] = row[14]
        meta['rna_type'] = row[15]
        meta['compartment'] = row[16]
        meta['bio_replicate'] = row[17]
        meta['partition'] = row[18]
        meta['annotation_version'] = row[19]
        meta['lab'] = row[20]
        meta['experimentid'] = get_experiment_id(confs, meta)

        if not raw:
            get_experiment_labels(meta, rna_types, cell_types, compartments)

        if experimentids.has_key(meta['experimentid']):
            experimentids[meta['experimentid']].append(meta)
        else:
            experimentids[meta['experimentid']] = [meta]

    results = []
    for key, value in experimentids.items():    
        results.append(get_experiment_result(confs, value))

    results.sort()

    chart['table_data'] = results
    return chart
