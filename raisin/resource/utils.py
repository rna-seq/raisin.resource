"""Utility methods for descriptive titles, level information and aggregation"""

from root import STATS_REGISTRY
from raisin.mysqldb import run_method_using_mysqldb
from restish import http


def get_rna_extract_display_mapping(dbs):
    """Query the RNA dasboard database for rna type labels"""
    sql = """
select ucscName, displayName
from rnaExtract"""
    dashboard_db = get_dashboard_db(dbs, 'hg19')
    if dashboard_db is None:
        return {}
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    mapping = {}
    for row in rows:
        mapping[row[0]] = row[1]

    # Add HBM project specific RNA Type, which is a ribosomal depleted RNA type
    if not 'RIBOFREE' in mapping:
        mapping['RIBOFREE'] = 'Ribosomal Free'

    return mapping


def get_cell_display_mapping(dbs):
    """Query the RNA dasboard database for cell type labels"""
    sql = """
select ucscName,
       displayName
from cell"""
    dashboard_db = get_dashboard_db(dbs, 'hg19')
    if dashboard_db is None:
        return {}
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    mapping = {}
    for row in rows:
        mapping[row[0]] = row[1]
    return mapping


def get_localization_display_mapping(dbs):
    """Query the RNA dasboard database for localization labels"""
    sql = """
select ucscName, displayName
from localization"""
    dashboard_db = get_dashboard_db(dbs, 'hg19')
    if dashboard_db is None:
        return {}
    cursor = dashboard_db.query(sql)
    rows = cursor.fetchall()
    cursor.close()
    mapping = {}
    for row in rows:
        mapping[row[0]] = row[1]
    return mapping


def get_parameter_list(confs, separator='-'):
    """Return the parameter list"""
    projectid = confs['kwargs']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    experimentid_parts = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        experimentid_parts.append(parameter)
    experimentid = separator.join(experimentid_parts)
    return experimentid


def get_parameter_values(confs, meta, separator='-'):
    """Return the parameter values"""
    projectid = confs['kwargs']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    meta['partition'] = meta['partition'] or '@'
    meta['bio_replicate'] = meta['bio_replicate'] or '1'
    meta['cell'] = meta['cell'] or '@'
    meta['localization'] = meta['localization'] or 'CELL'
    meta['rnaExtract'] = meta['rnaExtract'] or '@'
    meta['annotation_version'] = meta['annotation_version'] or '@'
    meta['lab'] = meta['lab'] or '@'
    meta['read_length'] = meta['read_length'] or '@'
    if meta['paired'] in [0, 1]:
        pass
    else:
        meta['paired'] = 0
    experimentid_parts = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        experimentid_parts.append(meta[parameter])
    experimentid = separator.join([str(part) for part in experimentid_parts])
    return experimentid


def get_experiment_dict(confs):
    """Make a dict out of the parameters defined in parameter_list and
    parameter_values.
    """

    projectid = confs['kwargs']['projectid']

    parameter_list = confs['kwargs']['parameter_list'].split('-')

    if not 'parameter_values' in confs['kwargs']:
        return {'projectid': projectid,
                'parameter_list': parameter_list
                }

    parameter_values = confs['kwargs']['parameter_values'].split('-')

    if len(parameter_list) != len(parameter_values):
        raise AttributeError

    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']

    meta = {'projectid': projectid,
            'parameter_list': parameter_list,
            'parameter_values': parameter_values}

    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        if parameter in parameter_list:
            # Take the parameter out of the parameter_values from the same
            # position as the parameter in the parameter_list.
            meta[parameter] = parameter_values[parameter_list.index(parameter)]
    return meta


def get_experiment_labels(meta, rna_extracts, cells, localizations):
    """Return experiment labels"""
    if meta['cell']  in cells:
        meta['cell'] = cells[meta['cell']]
    if meta['rnaExtract'] in rna_extracts:
        meta['rnaExtract'] = rna_extracts[meta['rnaExtract']]
    if meta['localization'] in localizations:
        meta['localization'] = localizations[meta['localization']]


def get_experiment_chart(confs):
    """Return experiment chart"""
    projectid = confs['kwargs']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    chart = {}
    chart['table_description'] = [('Project id',       'string'),
                                  ('Parameter List',   'string'),
                                  ('Parameter Values', 'string'),
                                  ('# Replicates',     'string'),
                                 ]
    # Either take the parameter mapping defined for the project
    # or take all parameters
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        chart['table_description'].append(parameter_labels[parameter])
    return chart


def get_experiment_result(confs, meta):
    """Return experiment results"""
    projectid = confs['kwargs']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    number_of_replicates = len(meta)
    meta = meta[0]
    experimentid_parts = [meta['projectid'],
                          meta['parameter_list'],
                          meta['parameter_values'],
                          str(number_of_replicates)]
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        experimentid_parts.append(meta[parameter])
    return experimentid_parts


def get_experiment_order_by(confs):
    """Return experiment order by"""
    projectid = confs['kwargs']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_columns = confs['request'].environ['parameter_columns']
    parameter_labels = confs['request'].environ['parameter_labels']
    order_by = ""
    bys = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        bys.append(parameter_columns[parameter])
    if bys:
        order_by = """
order by
    %s;""" % ',\n      '.join(bys)
    return order_by


def get_experiment_where(confs, meta):
    """Return experiment where clause"""
    projectid = meta['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_columns = confs['request'].environ['parameter_columns']
    parameter_labels = confs['request'].environ['parameter_labels']
    parameter_list = confs['kwargs']['parameter_list'].split('-')
    where = """where
%s
"""
    ands = ["project_id='%s'" % meta['projectid']]
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        if parameter in parameter_list:
            # Take the parameter out of the parameter_values from the same
            # position as the parameter in the parameter_list.
            if parameter in meta:
                key, value = parameter_columns[parameter], meta[parameter]
                ands.append("%s = '%s'" % (key, value))
    return where % ('\nand\n    '.join(ands))


def get_experiment_replicates(dbs, confs):
    """Return experiment replicates"""
    projectid = confs['kwargs']['projectid']
    if 'parameter_values' in confs['kwargs']:
        meta = get_experiment_dict(confs)
        sql = """
    select experiment_id
    from experiments
    %s
    order by
        experiment_id;""" % get_experiment_where(confs, meta)
        cursor = dbs[projectid]['RNAseqPipelineCommon'].query(sql)
    else:
        sql = """
    select experiment_id
    from experiments
    where project_id = '%s'
    order by
        experiment_id;""" % projectid
        cursor = dbs[projectid]['RNAseqPipelineCommon'].query(sql)

    rows = cursor.fetchall()
    cursor.close()
    replicateids = [row[0] for row in rows]
    return replicateids


def get_level(replicateid, laneid, readid):
    """Return level"""
    level = {}
    # Collect all valid additional parameters
    parameters = [replicateid, laneid, readid]
    level['parameters'] = [p for p in parameters if not p is None]
    # Define what level we are on depending on the number of paramaters
    level['id'] = {3: 'Read',
                   2: 'Lane',
                   1: 'Replicate',
                   0: 'Experiment'}[len(level['parameters'])]
    return level


def configurations_for_level(request, dbs, configurations, level):
    """Return configurations for level"""
    level_confs = []
    for kwargs in configurations:
        if level is None:
            return configurations
        elif level == 'project':
            return configurations
        elif level == 'experiment':
            items, success = run(dbs,
                                 get_project_experiments,
                                 {'kwargs': kwargs, 'request': request})
        elif level == 'replicate':
            items, success = run(dbs,
                                 get_experiment_replicates,
                                 {'kwargs': kwargs, 'request': request})
        elif level == 'lane':
            items, success = run(dbs, get_replicate_lanes, kwargs)
        elif level == 'read':
            items, success = run(dbs, get_lane_reads, kwargs)
        if success:
            for item in items:
                configuration = kwargs.copy()
                configuration["%sid" % level] = item
                level_confs.append(configuration)
    return level_confs


def partition_configurations(configurations, level):
    """Return partition configurations"""
    part_id = '%sid' % level
    part_confs = {}
    for conf in configurations:
        part = conf[part_id]
        if part in part_confs:
            part_confs[part].append(conf)
        else:
            part_confs[part] = [conf]
    return part_confs


def get_configurations(request, level, resolution, partition, dbs, **kwargs):
    """Return configurations"""
    level_titles = {'project':    'Project Id',
                    'experiment': 'Experiment Id',
                    'replicate':  'Replicate Id',
                    'lane':       'Lane Id',
                    'read':       'Read Id',
                    None: None,
                    }
    resolution_titles = {'project':    'Project Level',
                         'experiment': 'Experiment Level',
                         'replicate':  'Replicate Level',
                         'lane':       'Lane Level',
                         'read':       'Read Level',
                         None: None,
                        }
    # Create the configuration partitions for this level
    configurations = [kwargs.copy()]
    levels = [None, 'project', 'experiment', 'replicate', 'lane', 'read']
    partition_levels = {None: 'project',
                        'project': 'experiment',
                        'experiment': 'replicate',
                        'replicate': 'lane',
                        'lane': 'read',
                        'read': 'read'}
    level_range = levels[levels.index(level) + 1:levels.index(resolution) + 1]

    for configuration_level in level_range:
        configurations = configurations_for_level(request,
                                                  dbs,
                                                  configurations,
                                                  configuration_level)

    if partition:
        if level_range == []:
            configurations = partition_configurations(configurations,
                                                      resolution)
        else:
            configurations = partition_configurations(configurations,
                                                      level_range[0])

    result = {'request': request,
              'level': {
                       'id': level,
                       'title': level_titles[level]
                       },
              'resolution': {
                       'id': resolution,
                       'title': resolution_titles[resolution]
                       },
              'partition': partition,
              'partition_level': {
                       'id': partition_levels[level],
                       'title': level_titles[partition_levels[level]],
                       },
              'configurations': configurations,
              'kwargs': kwargs,
             }
    return result


def get_project_experiments(dbs, confs):
    """Return project experiments"""
    projectid = confs['kwargs']['projectid']
    sql = """
select project_id,
       CellType,
       RNAType,
       Compartment,
       Bioreplicate,
       partition,
       annotation_version,
       lab,
       read_length,
       paired
from experiments
where
      project_id='%(projectid)s'
""" % confs['kwargs']
    cursor = dbs[projectid]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    results = {}
    for row in rows:
        if row in results:
            pass
        else:
            meta = {'projectid': projectid,
                    'cell': row[1],
                    'rnaExtract': row[2],
                    'localization': row[3],
                    'bio_replicate': row[4],
                    'partition': row[5],
                    'annotation_version': row[6],
                    'lab': row[7],
                    'read_length': row[8],
                    'paired': row[9],
                   }
            if not meta['paired'] is None:
                meta['paired'] = ord(meta['paired'])
            meta['parameter_list'] = get_parameter_list(confs)
            meta['parameter_values'] = get_parameter_values(confs, meta)
            results[row] = meta['parameter_values']
    experiments = list(set(results.values()))
    experiments.sort()
    return experiments


def get_replicate_lanes(dbs, conf):
    """Return replicate lanes"""
    sql = """
select
    distinct pair_id
from
    %(projectid)s_%(replicateid)s_dataset
order by
    pair_id
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    laneids = [r[0] for r in rows]
    return laneids


def get_lane_reads(dbs, conf):
    """Return lane reads"""
    sql = """
select distinct
    lane_id
from
    %(projectid)s_%(replicateid)s_dataset
where
    pair_id = '%(laneid)s'
order by
    lane_id
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    readids = [r[0] for r in rows]
    return readids


def run(dbs, method, conf):
    """Run a method running sql code.

    Returns a tuple of (data, success).

    If the success value is True, the method could not be run properly.

    If the success value is False, the method has been executed correctly.
    """
    success = True
    data = run_method_using_mysqldb(method, dbs, conf, http.not_found)
    if data == http.not_found:
        print "Error running sql method."
        success = False
    return data, success


def aggregate(dbs, confs, method, strategy, **kwargs):
    """Aggregate results from multiple queries to the database using
    a strategy."""
    stats = None
    failed = 0
    for conf in confs:
        conf.update(kwargs)
        data = run_method_using_mysqldb(method, dbs, conf, http.not_found)
        if data == http.not_found:
            print "Can't aggregate because of missing data."
            failed = failed + 1
        else:
            if stats is None:
                stats = data
            else:
                stats = merge(stats, data, strategy=strategy)
    return stats, failed


def collect(dbs, confs, method, strategy, **kwargs):
    """Collect results from multiple queries to the database using
    a strategy."""
    results = []
    for conf in confs:
        conf.update(kwargs)
        data = run_method_using_mysqldb(method, dbs, conf, http.not_found)
        if data == http.not_found:
            print "Can't collect because of missing data."
        else:
            for line in data:
                results.append(strategy(conf, line))
    return results


def merge(d_1, d_2, strategy=None):
    """
    http://stackoverflow.com/questions/38987/how-can-i-merge-two-python-dictionaries-as-a-single-expression

    Merges two dictionaries, non-destructively, combining
    values on duplicate keys as defined by the optional strategy
    function.  The default behavior replaces the values in d1
    with corresponding values in d2.  (There is no other generally
    applicable merge strategy, but often you'll have homogeneous
    types in your dicts, so specifying a merge technique can be
    valuable.)

    Examples:

    >>> d_1
    {'a': 1, 'c': 3, 'b': 2}
    >>> merge(d_1, d_1)
    {'a': 1, 'c': 3, 'b': 2}
    >>> merge(d_1, d_1, lambda x,y: x+y)
    {'a': 2, 'c': 6, 'b': 4}
    """
    if strategy is None:
        strategy = lambda x, y: y
    result = dict(d_1)
    for key, value in d_2.iteritems():
        if key in result:
            result[key] = strategy(result[key], value)
        else:
            result[key] = value
    return result


class register_resource(object):
    """
    A resource has a level of granularity at which it is stored in the database

    For example, statistics that are calculated for reads can be shown

    - as simple read statistics on a read level

    - as aggregated or partitioned on higher levels

    The summary can either be done by aggregating the totals or by showing the
    partition.

    For example, for a read level statistic, it can be shown as

    - a read

    - a lane with reads when partition is True

    - a lane aggregating the reads when partition is False
    """
    # pylint: disable-msg=C0103
    # This class is used as a decorator, so allow lower case name

    def __init__(self, resolution, partition):
        self.resolution = resolution
        self.partition = partition

    def __call__(self, wrapped=None):
        if not wrapped:
            return
        # All levels at which there are statistics in the database
        levels = [None, 'project', 'experiment', 'replicate', 'lane', 'read']
        # Only levels that are higher than the base resolution
        # level are possible
        for level in levels[:levels.index(self.resolution) + 1]:
            # Every statistic is known under its name prefixed with
            # the level for which it is summarized
            if level == None:
                # This is a top level resource, so no prefix needed
                key = "%s" % wrapped.__name__
            else:
                key = "%s_%s" % (level, wrapped.__name__)
            # Now store the new statistics level in the registry
            value = (wrapped, level, self.resolution, self.partition)
            STATS_REGISTRY[key] = value


def get_dashboard_db(dbs, hgversion):
    """Get the dashboard database for this human genome version"""
    if hgversion == 'hg19':
        dashboard_db = 'hg19_RNA_dashboard'
    elif hgversion == 'hg18':
        dashboard_db = 'hg18_RNA_dashboard'
    else:
        raise AttributeError
    # Search for the first matching db. This is done because we are not given
    # a specific project, and want to avoid hard-coding the project from which
    # to take the db
    for project_dbs in dbs.values():
        if dashboard_db in project_dbs:
            return project_dbs[dashboard_db]


def to_cfg(data):
    """Make a .cfg file out of a table, so it becomes usable by buildout."""
    description_keys = [d[0] for d in data['table_description']]
    by_id = {}
    for row in data['table_data']:
        row_dict = dict(zip(description_keys, row))
        if row[0] in by_id:
            by_id[row[0]].append(row_dict)
        else:
            by_id[row[0]] = [row_dict]

    ids = list(by_id.keys())
    ids.sort()
    parts = []
    for partid in ids:
        parts.append("[%s]" % partid)
        for key in description_keys[1:]:
            parts.append("%s=%s" % (key, by_id[partid][0][key]))
            if len(by_id[partid]) > 1:
                for row_dict in by_id[partid][1:]:
                    parts.append(" " * (len(key) + 1) + str(row_dict[key]))
        parts.append("")
    return "\n".join(parts)
