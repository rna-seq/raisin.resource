"""Run related resources"""

from utils import get_rna_type_display_mapping
from utils import get_cell_type_display_mapping
from utils import get_compartment_display_mapping
from utils import get_parameter_list
from utils import get_replicate_where
from utils import get_replicate_dict
from utils import get_parameter_values
from utils import register_resource


@register_resource(resolution="experiment", partition=False)
def experiment_info(dbs, confs):
    """Collect some general information about a experiment"""
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
                                 ]
    result = []
    conf = confs['configurations'][0]
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
where project_id='%(projectid)s'
      and experiment_id='%(experimentid)s'""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()

    species_id = rows[0][2]
    genome_id = rows[0][3]
    annotation_id = rows[0][4]
    result.append(int(rows[0][6]))
    result.append(int(rows[0][7]))
    result.append(rows[0][8])
    result.append(str(rows[0][9]))
    # Use labels instead of the raw values
    result.append(get_cell_type_display_mapping(dbs).get(rows[0][10],
                                                         rows[0][10]))
    result.append(get_rna_type_display_mapping(dbs).get(rows[0][11],
                                                        rows[0][11]))
    result.append(get_compartment_display_mapping(dbs).get(rows[0][12],
                                                           rows[0][12]))
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
    chart['table_data'] = [result, ]
    return chart


@register_resource(resolution=None, partition=False)
def project_experiments(dbs, confs):
    """Compile the list of experiments for the project"""
    conf = confs['configurations'][0]
    projectid = conf['projectid']

    chart = {}
    chart['table_description'] = [('Project Id',               'string'),
                                  ('Experiment Id',            'string'),
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

    url = ('/project/%(projectid)s/'
           '%(parameter_list)s/%(parameter_values)s/'
           'experiment/%(experimentid)s/tab/overview')
    results = []
    for row in rows:
        row = list(row)
        if not row[22] is None:
            row[22] = ord(row[22])
        # Augment the information from the database with a url and a text
        meta = {'projectid': row[0],
                'experimentid': row[1],
                'read_length': row[11],
                'cell_type': row[15],
                'rna_type': row[16],
                'compartment': row[17],
                'bio_replicate': row[18],
                'partition': row[19],
                'annotation_version': row[20],
                'lab': row[21],
                'paired': row[22],
               }
        meta['parameter_list'] = get_parameter_list(confs)
        meta['parameter_values'] = get_parameter_values(confs, meta)
        results.append(row + [url % meta])
    chart['table_data'] = results
    return chart


@register_resource(resolution="project", partition=False)
def project_accessions(dbs, confs):
    """Produce accessions with information obtained from the pipeline database

    The accession file can be fetched like this to fetch all accessions for the lab CSHL

        curl -H "Accept:text/x-cfg" http://localhost:6464/project/ENCODE/accessions
    """
    projectid = confs['configurations'][0]['projectid']
    chart = {}
    chart['table_description'] = [('accession',               'string'),
                                  ('species',                 'string'),
                                  ('rnaExtract',              'string'),
                                  ('localization',            'string'),
                                  ('replicate',               'string'),
                                  ('gender',                  'string'),
                                  ('readType',                'string'),
                                  ('cell',                    'string'),
                                 ]

    sql = """
select experiment_id,
       species_info.species,
       RNAType,
       Compartment,
       Bioreplicate,
       genome_files.gender,
       read_length,
       CellType
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
    cursor = dbs[projectid]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()

    results = []
    for row in rows:
        results.append(row)
    chart['table_data'] = results
    print chart
    return chart


@register_resource(resolution=None, partition=False)
def replicate_experiments(dbs, confs):
    """Compile the list of experiments for the replicate"""
    chart = {}
    chart['table_description'] = [('Project Id',       'string'),
                                  ('Parameter List',   'string'),
                                  ('Parameter Values', 'string'),
                                  ('Experiment Id',    'string'),
                                  ('Experiment Url',   'string'),
                                 ]
    projectid = confs['kwargs']['projectid']
    parameter_list = confs['kwargs']['parameter_list']
    parameter_values = confs['kwargs']['parameter_values']
    meta = get_replicate_dict(confs)
    # Only return the replicate infos if this is an official project
    sql = """
select experiment_id
from experiments
%s
order by
    experiment_id;""" % get_replicate_where(confs, meta)
    cursor = dbs[projectid]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    experimentids = [row[0] for row in rows]
    results = []
    url = '/project/%s/%s/%s/experiment/%s/tab/overview'
    for experimentid in experimentids:
        results.append((projectid,
                        parameter_list,
                        parameter_values,
                        experimentid,
                        url % (projectid,
                               parameter_list,
                               parameter_values,
                               experimentid),
                       )
                      )
    chart['table_data'] = results
    return chart
