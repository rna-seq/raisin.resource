from utils import run
from utils import get_rna_type_display_mapping
from utils import get_cell_type_display_mapping
from utils import get_compartment_display_mapping
from utils import get_experiment_chart
from utils import get_parameter_list
from utils import get_parameter_values
from utils import get_experiment_dict
from utils import get_experiment_result
from utils import get_experiment_order_by
from utils import get_experiment_labels
from utils import get_experiment_where
from utils import register_resource

@register_resource(resolution=None, partition=False)      
def experiment_info(dbs, confs):
    conf = confs['configurations'][0]
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
                                  ('Paired',             'number'),
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
       partition,
       paired
from experiments 
%s
order by 
    experiment_id;""" % get_experiment_where(confs, meta)
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
    result.append(rows[0][15])
    if not result[-1] is None:
        result[-1] = ord(result[-1])

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
    if rows[0][0] == 'Ging001N':
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
    paired:        Experiments can be done for paired end
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
                                  ('Paired',                   'number'),
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
       partition,
       paired
from experiments;""" 
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    results = []
    for row in rows:
        row = list(row)
        if not row[8] is None:
            row[8] = ord(row[8])
        results.append(row) 
    return results

@register_resource(resolution='project', partition=False)
def project_experiments(dbs, confs):
    conf = confs['configurations'][0]
    projectid = conf['projectid']

    chart = {}
    chart['table_description'] = [('Project Id',               'string'),
                                  ('Run Id',                   'string'),
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
                                  ('Annotation Version',       'string'),
                                  ('Lab',                      'string'),
                                  ('Paired',                   'number'),
                                  ('URL',                      'string'),
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
       lab,
       paired
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
    url = '/project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/statistics/experiments'
    for row in rows:
        # Augment the information from the database with a url and a text
        row = list(row)
        if not row[22] is None:
            row[22] = ord(row[22])
        meta = {'projectid':row[0],
                'read_length':row[11],
                'cell_type':row[15],
                'rna_type':row[16],
                'compartment':row[17],
                'bio_replicate':row[18],
                'partition':row[19],
                'annotation_version':row[20],
                'lab':row[21],
                'paired':row[22]}
        meta['parameter_list'] = get_parameter_list(confs, meta)
        meta['parameter_values'] = get_parameter_values(confs, meta)
        row.append(url % meta)
        results.append(row)
    chart['table_data'] = results
    return chart

@register_resource(resolution='project', partition=False)
def project_experiment_subset(dbs, confs):
    return _project_experimentstable(dbs, confs, raw=True, where=True)

@register_resource(resolution='project', partition=False)
def project_experimentstableraw(dbs, confs):
    return _project_experimentstable(dbs, confs, raw=True, where=False)
    
@register_resource(resolution='project', partition=False)
def project_experimentstable(dbs, confs):
    return _project_experimentstable(dbs, confs, raw=False, where=False)

def _project_experimentstable(dbs, confs, raw=True, where=False):
    chart = get_experiment_chart(confs)
    experimentids = _project_experimentstable_experiments(dbs, confs, raw, where)
    results = []
    for key, value in experimentids.items():    
        results.append(get_experiment_result(confs, value))
    results.sort()
    chart['table_data'] = results
    return chart

def _project_experimentstable_experiments(dbs, confs, raw=True, where=False):
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
       lab,
       paired
from experiments,
     species_info,
     genome_files,
     annotation_files
"""
    if where:
        meta = get_experiment_dict(confs)
        sql = """%s
%s
and
""" % (sql, get_experiment_where(confs, meta))
    else:
        sql = """%s
where
    project_id = '%s'
and
""" % (sql, conf['projectid'])

    sql = """%s
      experiments.species_id = species_info.species_id
and
      experiments.genome_id = genome_files.genome_id
and 
      experiments.annotation_id = annotation_files.annotation_id
""" % sql

    sql = """%s
%s""" % (sql, get_experiment_order_by(confs))

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
        meta['read_length'] = row[10]
        meta['cell_type'] = row[14]
        meta['rna_type'] = row[15]
        meta['compartment'] = row[16]
        meta['bio_replicate'] = row[17]
        meta['partition'] = row[18]
        meta['annotation_version'] = row[19]
        meta['lab'] = row[20]
        meta['paired'] = row[21]
        if not meta['paired'] is None:
            meta['paired'] = ord(meta['paired'])
        meta['parameter_list'] = get_parameter_list(confs, meta)
        meta['parameter_values'] = get_parameter_values(confs, meta)

        if not raw:
            get_experiment_labels(meta, rna_types, cell_types, compartments)

        if experimentids.has_key(meta['parameter_values']):
            experimentids[meta['parameter_values']].append(meta)
        else:
            experimentids[meta['parameter_values']] = [meta]
    return experimentids
    
@register_resource(resolution='project', partition=False)
def project_experiment_subset_selection(dbs, confs):
    experimentids = _project_experimentstable_experiments(dbs, confs, raw=True, where=True)    
    conf = confs['configurations'][0]
    projectid = conf['projectid']
    table = _project_experimentstable(dbs, confs, raw=True, where=True)
    meta = get_experiment_dict(confs)
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']

    subsets = []
    supersets = []
    for parameter in parameter_mapping[projectid]:
        if parameter in meta['parameter_list']:
            if meta.has_key(parameter):
                supersets.append(parameter) 
        else:
            if not meta.has_key(parameter):
                subsets.append(parameter) 

    variations = {}
    variation_count = {}
    for experiment_list in experimentids.values():
        for parameter in parameter_mapping[projectid]:
            if variation_count.has_key(parameter):
                variation_count[parameter].append(experiment_list[0][parameter])
            else:
                variation_count[parameter] = [experiment_list[0][parameter]]
            for experiment in experiment_list:
                if parameter in experiment:
                    if variations.has_key(parameter):
                        variations[parameter].add(experiment[parameter])
                    else:
                        variations[parameter] = set([experiment[parameter]])

    links = []

    for subset in subsets:
        if len(variations[subset]) > 1:
            for variation in variations[subset]:
                links.append(('%s-%s' % (confs['kw']['parameter_list'], subset), 
                              '%s-%s' % (confs['kw']['parameter_values'], variation),
                              parameter_labels[subset][0],
                              variation,
                              subset,
                              ))
    
    chart = {}
    chart['table_description'] = [('Project',               'string'),
                                  ('Parameter Names',       'string'),
                                  ('Parameter Values',      'string'),
                                  ('Parameter Type',        'string'),
                                  ('Parameter Value',       'string'),
                                  ('Number of experiments for this Parameter Value', 'string'),
                                 ]    
    chart['table_data'] = []
    for names, values, name, value, subset in links:
        chart['table_data'].append ( (projectid,  names, values, name, str(value), str(variation_count[subset].count(value))) )

    return chart
