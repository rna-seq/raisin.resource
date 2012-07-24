"""Experiment level resources"""

from utils import run
from utils import get_rna_extract_display_mapping
from utils import get_cell_display_mapping
from utils import get_localization_display_mapping
from utils import get_experiment_chart
from utils import get_parameter_list
from utils import get_parameter_values
from utils import get_experiment_dict
from utils import get_experiment_result
from utils import get_experiment_order_by
from utils import get_experiment_labels
from utils import get_experiment_where
from utils import register_resource

from project import rnadashboard_results_pending


@register_resource(resolution=None, partition=False)
def experiment_info(dbs, confs):
    """XXX Needs refactoring"""
    conf = confs['configurations'][0]
    chart = {}
    chart['table_description'] = [('Read Length',        'number'),
                                  ('Mismatches',         'number'),
                                  ('Description',        'string'),
                                  ('Date',               'string'),
                                  ('Cell Type',          'string'),
                                  ('RNA Type',           'string'),
                                  ('Localization',       'string'),
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
    result.append(get_cell_display_mapping(dbs).get(rows[0][10],
                                                         rows[0][10]))
    result.append(get_rna_extract_display_mapping(dbs).get(rows[0][11],
                                                        rows[0][11]))
    result.append(get_localization_display_mapping(dbs).get(rows[0][12],
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
def experiments(dbs, confs):
    """Used for generating the buildout.cfg for pipeline.buildout."""
    # pylint: disable-msg=W0613
    # Configurations are not used here
    chart = {}
    chart['table_description'] = [('Project id',               'string'),
                                  ('Replicate id',             'string'),
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
        rows, success = run(dbs, _experiments, {'projectid': projectid})
        if success:
            results = results + list(rows)

    chart['table_data'] = results
    return chart


def _experiments(dbs, conf):
    """Query the database for a list of experiments."""
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
def replicates_configurations(dbs, confs):
    """
    The replicates have a number of configuration parameters that define
    them uniquely.

    project_id:    Defines what project this replicate was made for
    replicate_id: Unique identifier of the replicate
    read_length:   Replicates in a project can have different read lengths
    CellType:      Replicates may come from different cell types
    RNAType:       Replicates may have been done with different rna types
    Localization:  Replicates may have been prepared from different cell localizations
    Bioreplicate:  Replicates are done for a bio experiment
    partition:     Replicates can be done for samples coming from different conditions
    paired:        Replicates can be done for paired end
    """
    # pylint: disable-msg=W0613
    # The configurations are not taken into account here.
    chart = {}
    chart['table_description'] = [('Project id',               'string'),
                                  ('Replicate id',             'string'),
                                  ('Read Length',              'number'),
                                  ('Cell Type',                'string'),
                                  ('RNA Type',                 'string'),
                                  ('Localization',             'string'),
                                  ('Bio Replicate',            'string'),
                                  ('Partition',                'string'),
                                  ('Paired',                   'number'),
                                 ]

    results = []
    for projectid in dbs.keys():
        rows, success = run(dbs,
                            _replicates_configurations,
                            {'projectid': projectid})
        if success:
            results = results + list(rows)

    chart['table_data'] = results
    return chart


def _replicates_configurations(dbs, conf):
    """Query the database for a list of replicate configurations."""
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
    """Query the database for a list of experiments for a project."""
    conf = confs['configurations'][0]
    projectid = conf['projectid']

    chart = {}
    chart['table_description'] = [('Project Id',               'string'),
                                  ('Replicate Id',             'string'),
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
                                  ('Replicate Description',    'string'),
                                  ('Replicate Date',           'string'),
                                  ('Cell Type',                'string'),
                                  ('RNA Type',                 'string'),
                                  ('Localization',             'string'),
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
    url = '/project/%(projectid)s/'
    url += '%(parameter_list)s/%(parameter_values)s'
    for row in rows:
        # Augment the information from the database with a url and a text
        row = list(row)
        if not row[22] is None:
            row[22] = ord(row[22])
        meta = {'projectid': row[0],
                'read_length': row[11],
                'cell': row[15],
                'rna_extract': row[16],
                'localization': row[17],
                'bio_replicate': row[18],
                'partition': row[19],
                'annotation_version': row[20],
                'lab': row[21],
                'paired': row[22]}
        meta['parameter_list'] = get_parameter_list(confs)
        meta['parameter_values'] = get_parameter_values(confs, meta)
        row.append(url % meta)
        results.append(row)
    chart['table_data'] = results
    return chart


@register_resource(resolution='project', partition=False)
def project_experiment_subset(dbs, confs):
    """Return a subset of experiments for a project."""
    return _project_experimentstable(dbs, confs, raw=True, where=True)


@register_resource(resolution='project', partition=False)
def project_experimentstableraw(dbs, confs):
    """Return a list of experiments for a project using raw values."""
    return _project_experimentstable(dbs, confs, raw=True, where=False)


@register_resource(resolution='project', partition=False)
def project_experimentstable(dbs, confs):
    """Return a list of experiments for a project."""
    return _project_experimentstable(dbs, confs, raw=False, where=False)


def _project_experimentstable(dbs, confs, raw=True, where=False):
    """Return a list of experiments for a project."""
    chart = get_experiment_chart(confs)
    experimentids = _project_experimentstable_experiments(dbs,
                                                          confs,
                                                          raw,
                                                          where)
    results = []
    for value in experimentids.values():
        results.append(get_experiment_result(confs, value))
    results.sort()

    if len(results) == 0:
        results = [[None] * len(chart['table_description'])]

    chart['table_data'] = results
    return chart


def _project_experimentstable_experiments(dbs, confs, raw=True, where=False):
    """Return a list of experiments for a project."""
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

    rna_extracts = get_rna_extract_display_mapping(dbs)
    cells = get_cell_display_mapping(dbs)
    localizations = get_localization_display_mapping(dbs)

    for row in rows:
        meta = {}
        meta['projectid'] = conf['projectid']
        meta['read_length'] = row[10]
        meta['cell'] = row[14]
        meta['rnaExtract'] = row[15]
        meta['localization'] = row[16]
        meta['bio_replicate'] = row[17]
        meta['partition'] = row[18]
        meta['annotation_version'] = row[19]
        meta['lab'] = row[20]
        meta['paired'] = row[21]
        if not meta['paired'] is None:
            meta['paired'] = ord(meta['paired'])
        meta['parameter_list'] = get_parameter_list(confs)
        meta['parameter_values'] = get_parameter_values(confs, meta)

        if not raw:
            get_experiment_labels(meta, rna_extracts, cells, localizations)

        if meta['parameter_values'] in experimentids:
            experimentids[meta['parameter_values']].append(meta)
        else:
            experimentids[meta['parameter_values']] = [meta]
    return experimentids


@register_resource(resolution='project', partition=False)
def project_experiment_subset_selection(dbs, confs):
    """XXX Needs refactoring"""
    experimentids = _project_experimentstable_experiments(dbs,
                                                          confs,
                                                          raw=True,
                                                          where=True)
    conf = confs['configurations'][0]
    projectid = conf['projectid']
    meta = get_experiment_dict(confs)
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']

    subsets = []
    supersets = []
    for parameter in parameter_mapping[projectid]:
        if parameter in meta['parameter_list']:
            if parameter in meta:
                supersets.append(parameter)
        else:
            if not parameter in meta:
                subsets.append(parameter)

    variations = {}
    variation_count = {}
    for experiment_list in experimentids.values():
        for parameter in parameter_mapping[projectid]:
            if parameter in variation_count:
                variation_count[parameter].append(experiment_list[0][parameter])
            else:
                variation_count[parameter] = [experiment_list[0][parameter]]
            for experiment in experiment_list:
                if parameter in experiment:
                    if parameter in variations:
                        variations[parameter].add(experiment[parameter])
                    else:
                        variations[parameter] = set([experiment[parameter]])

    links = []

    for subset in subsets:
        # If there is variation for this subset, add links
        if not subset in variations:
            continue
        if len(variations[subset]) < 2:
            continue
        for variation in variations[subset]:
            link = ('%s-%s' % (confs['kwargs']['parameter_list'], subset),
                   '%s-%s' % (confs['kwargs']['parameter_values'], variation),
                   parameter_labels[subset][0],
                   variation,
                   subset,
                  )
            links.append(link)

    chart = {}
    description = [('Project',                             'string'),
                   ('Parameter Names',                     'string'),
                   ('Parameter Values',                    'string'),
                   ('Parameter Type',                      'string'),
                   ('Parameter Value',                     'string'),
                   ('Replicates for this Parameter Value', 'string'),
                  ]
    chart['table_description'] = description
    chart['table_data'] = []
    for names, values, name, value, subset in links:
        chart['table_data'].append((projectid,
                                    names,
                                    values,
                                    name,
                                    str(value),
                                    str(variation_count[subset].count(value))))

    if len(chart['table_data']) == 0:
        chart['table_data'].append([None] * len(chart['table_description']))

    return chart


@register_resource(resolution='project', partition=False)
def project_experiment_subset_start(dbs, confs):
    """XXX This is not used yet

    The idea is to use this as a start for searching the parameter space
    of a project.
    """
    experimentids = _project_experimentstable_experiments(dbs,
                                                          confs,
                                                          raw=True,
                                                          where=True)
    conf = confs['configurations'][0]
    projectid = conf['projectid']
    meta = get_experiment_dict(confs)
    parameter_labels = confs['request'].environ['parameter_labels']

    variations = {}
    variation_count = {}
    for experiment_list in experimentids.values():
        for parameter in meta['parameter_list']:
            if parameter in variation_count:
                variation_count[parameter].append(experiment_list[0][parameter])
            else:
                variation_count[parameter] = [experiment_list[0][parameter]]
            for experiment in experiment_list:
                if parameter in experiment:
                    if parameter in variations:
                        variations[parameter].add(experiment[parameter])
                    else:
                        variations[parameter] = set([experiment[parameter]])

    links = []

    for parameter in meta['parameter_list']:
        for variation in variations[parameter]:
            link = (confs['kwargs']['parameter_list'],
                    parameter_labels[parameter][0],
                    variation,
                    parameter,
                  )
            links.append(link)

    chart = {}
    description = [('Project',                              'string'),
                   ('Parameter Names',                      'string'),
                   ('Parameter Values',                     'string'),
                   ('Parameter Type',                       'string'),
                   ('Parameter Value',                      'string'),
                   ('Replicates for this Parameter Value',  'string'),
                  ]
    chart['table_description'] = description
    chart['table_data'] = []

    for names, name, value, subset in links:
        chart['table_data'].append((projectid,
                                    names,
                                    name,
                                    str(value),
                                    str(variation_count[subset].count(value))))

    if len(chart['table_data']) == 0:
        chart['table_data'].append([None] * len(chart['table_description']))

    return chart


@register_resource(resolution='project', partition=False)
def project_experiment_subset_pending(dbs, confs):
    """Return a subset of pending experiments for a project."""
    confs['configurations'][0]['hgversion'] = 'hg19'
    dashboard = rnadashboard_results_pending(dbs, confs)
    grape = _project_experimentstable_experiments(dbs,
                                                  confs,
                                                  raw=True,
                                                  where=True)
    meta = get_experiment_dict(confs)
    parameter_labels = confs['request'].environ['parameter_labels']

    chart = {}
    description = [('Replicate',   'string'),
                   ('Lab',         'string'),
                   ('Cell Type',   'string'),
                   ('Localization', 'string'),
                   ('RNA Type',    'string'),
                   ('Read Length', 'string'),
                   ('Paired',      'string'),
                  ]

    results = []

    grape_set = set(grape.keys())
    dashboard_set = set(dashboard.keys())
    for key in dashboard_set.difference(grape_set):
        item = dashboard[key]
        item['RNA Type'] = item['RNA Extract Id']
        item['Localization'] = item['Localization Id']
        item['Lab'] = item['Replicate Lab']
        filter_out = False
        index = 0
        for parameter in meta['parameter_list']:
            if parameter in parameter_labels:
                value = item[parameter_labels[parameter][0]]
            else:
                value = None
            if value != meta['parameter_values'][index]:
                filter_out = True
            index += 1
        if not filter_out:
            results.append((key,
                           item['Replicate Lab'],
                           item['Cell Type'],
                           item['Localization'],
                           item['RNA Extract'],
                           item['Read Length'],
                           item['Paired']))
    chart['table_description'] = description
    if len(results) == 0:
        results = [(None,) * len(description)]

    chart['table_data'] = results

    return chart
