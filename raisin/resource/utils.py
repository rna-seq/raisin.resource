from root import stats_registry
from raisin.mysqldb import run_method_using_mysqldb
from restish import http

def get_rna_type_display_mapping(dbs):
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
    if not mapping.has_key('RIBOFREE'):
        mapping['RIBOFREE'] = 'Ribosomal Free'
    
    return mapping

def get_cell_type_display_mapping(dbs):
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

def get_compartment_display_mapping(dbs):
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

def get_parameter_list(confs, meta, separator = '-'):
    projectid = confs['kw']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    experimentid_parts = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        experimentid_parts.append(parameter)
    experimentid = separator.join(experimentid_parts)
    return experimentid

def get_parameter_values(confs, meta, separator = '-'):
    projectid = confs['kw']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_columns = confs['request'].environ['parameter_columns']
    parameter_labels = confs['request'].environ['parameter_labels']
    meta['partition'] = meta['partition'] or '@'
    meta['bio_replicate'] = meta['bio_replicate'] or '1'
    meta['cell_type'] = meta['cell_type'] or '@'
    meta['compartment'] = meta['compartment'] or 'CELL'
    meta['rna_type'] = meta['rna_type'] or '@'
    meta['annotation_version'] = meta['annotation_version'] or '@'
    meta['lab'] = meta['lab'] or '@'
    meta['read_length'] = meta['read_length'] or '@'
    meta['paired'] = meta['paired'] or '@'
    experimentid_parts = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        experimentid_parts.append(meta[parameter])
    experimentid = separator.join([str(part) for part in experimentid_parts])
    return experimentid

def get_experiment_dict(confs):
    """
    Make a dict out of an experiment id
    """
    projectid = confs['kw']['projectid']
    parameter_list = confs['kw']['parameter_list']
    parameter_values = confs['kw']['parameter_values']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    elements = parameter_values.split('-')
    meta = {'projectid':projectid,
            'parameter_list':parameter_list,
            'parameter_values':parameter_values}
    i = 0
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        meta[parameter] = elements[i]
        i = i + 1
    return meta

def get_experiment_labels(meta, rna_types, cell_types, compartments):
    if meta['cell_type']  in cell_types:
        meta['cell_type']  = cell_types[meta['cell_type'] ]
    if meta['rna_type']  in rna_types:
        meta['rna_type']  = rna_types[meta['rna_type'] ]
    if meta['compartment']  in compartments:
        meta['compartment']  = compartments[meta['compartment'] ]

def get_experiment_chart(confs):
    projectid = confs['kw']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    chart = {}
    chart['table_description'] = [('Project id',       'string'),
                                  ('Parameter List',   'string'),
                                  ('Parameter Values', 'string'),
                                  ('# Runs',           'string'),
                                 ]
    # Either take the parameter mapping defined for the project or take all parameters
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        chart['table_description'].append(parameter_labels[parameter])
    return chart

def get_experiment_result(confs, meta):
    projectid = confs['kw']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_labels = confs['request'].environ['parameter_labels']
    number_of_runs = len(meta)
    meta = meta[0]
    experimentid_parts = [meta['projectid'], meta['parameter_list'], meta['parameter_values'], str(number_of_runs)]
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        experimentid_parts.append(meta[parameter])
    return experimentid_parts

def get_experiment_order_by(confs):
    projectid = confs['kw']['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_columns = confs['request'].environ['parameter_columns']
    parameter_labels = confs['request'].environ['parameter_labels']
    order_by = ""
    by = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        by.append(parameter_columns[parameter])
    if by:
        order_by = """
order by
    %s;""" % ',\n      '.join(by)
    return order_by
    
def get_experiment_where(confs, meta):
    projectid = meta['projectid']
    parameter_mapping = confs['request'].environ['parameter_mapping']
    parameter_columns = confs['request'].environ['parameter_columns']
    parameter_labels = confs['request'].environ['parameter_labels']
    where = """where 
    project_id='%s'
and
    %s
order by 
    experiment_id;"""
    ands = []
    for parameter in parameter_mapping.get(projectid, parameter_labels.keys()):
        ands.append("%s = '%s'" % (parameter_columns[parameter], meta[parameter]))
    return where % (meta['projectid'], '\nand\n    '.join(ands))

def get_experiment_runs(dbs, confs):
    meta = get_experiment_dict(confs)
    # Only return the experiment infos if this is an official project
    sql = """
select experiment_id
from experiments
%s""" % get_experiment_where(confs, meta)
    cursor = dbs[confs['kw']['projectid']]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    runids = [row[0] for row in rows]
    return runids
     
def get_level(runid, laneid, readid):
    level = {}
    # Collect all valid additional parameters
    level['parameters'] = [p for p in [runid, laneid, readid] if not p is None]
    # Define what level we are on depending on the number of paramaters
    level['id'] = {3:'Read', 
                   2:'Lane', 
                   1:'Run',
                   0:'Experiment'}[len(level['parameters'])]
    return level

def configurations_for_level(request, dbs, configurations, level):
    configurations_for_level = []
    for kw in configurations:
        if level is None:
            return configurations
        elif level == 'project':
            return configurations
        elif level == 'experiment':
            items, failed = run(dbs, get_project_experiments, {'kw':kw, 'request':request})
        elif level == 'run':
            items, failed = run(dbs, get_experiment_runs, {'kw':kw, 'request':request})
        elif level == 'lane':
            items, failed = run(dbs, get_run_lanes, kw)
        elif level == 'read':
            items, failed = run(dbs, get_lane_reads, kw)
        if failed:
            pass # Failed to get information for this configuration
        else:
            for item in items:
                configuration = kw.copy()
                configuration["%sid"%level] = item 
                configurations_for_level.append(configuration)
    return configurations_for_level

def partition_configurations(configurations, level):
    partition_id = '%sid' % level
    partition_configurations = {}
    for configuration in configurations:
        if partition_configurations.has_key(configuration[partition_id]):
            partition_configurations[configuration[partition_id]].append(configuration)
        else:
            partition_configurations[configuration[partition_id]] = [configuration]
    return partition_configurations

def get_configurations(request, level, resolution, partition, dbs, **kw):
    level_titles = {'project':    'Project Id',
                    'experiment': 'Experiment Id',
                    'run':        'Run Id',
                    'lane':       'Lane Id',
                    'read':       'Read Id',
                    None: None,
                    }
    resolution_titles = {'project':    'Project Level',
                          'experiment': 'Experiment Level',
                          'run':        'Run Level',
                          'lane':       'Lane Level',
                          'read':       'Read Level',
                          None: None,
                         }
    # Create the configuration partitions for this level
    configurations = [kw.copy()]
    levels = [None, 'project', 'experiment', 'run', 'lane', 'read']
    partition_levels = {None:'project',
                        'project':'experiment',
                        'experiment':'run',
                        'run':'lane',
                        'lane':'read',
                        'read':'read'}
    level_range = levels[levels.index(level)+1:levels.index(resolution)+1]
    #print "Choosing configurations for \n\trange: %s" % level_range 
        
    for configuration_level in level_range:
        configurations = configurations_for_level(request, dbs, configurations, configuration_level)
    
    if partition:
        if level_range == []:
            configurations = partition_configurations(configurations, resolution)
        else:
            configurations = partition_configurations(configurations, level_range[0])
    
    result = {'request': request,
              'level':{
                       'id':level,
                       'title':level_titles[level]
                       },
              'resolution':{
                       'id':resolution,
                       'title':resolution_titles[resolution]
                       },
              'partition':partition,
              'partition_level':{
                       'id':partition_levels[level],
                       'title':level_titles[partition_levels[level]],
                       },
              'configurations':configurations, 
              'kw': kw,
             }
    return result

def get_project_experiments(dbs, confs):
    projectid = confs['kw']['projectid']
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
""" % confs['kw']
    cursor = dbs[projectid]['RNAseqPipelineCommon'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    results = {}
    for row in rows:
        if results.has_key(row):
            pass
        else:
            results[row] = get_experiment_id(confs, 
                                             {'projectid':projectid,
                                              'cell_type':row[1],
                                              'rna_type':row[2],
                                              'compartment':row[3],
                                              'bio_replicate':row[4],
                                              'partition':row[5],
                                              'annotation_version':row[6],
                                              'lab':row[7],
                                              'read_length':row[8],
                                              'paired':row[8],
                                              } )
    experiments = list(set(results.values()))
    experiments.sort()
    return experiments
    
def get_run_lanes(dbs, conf):
    sql = """
select 
    distinct pair_id
from 
    %(projectid)s_%(runid)s_dataset
order by
    pair_id
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result = []
    laneids = [r[0] for r in rows]
    return laneids

def get_lane_reads(dbs, conf):
    sql = """
select distinct 
    lane_id
from 
    %(projectid)s_%(runid)s_dataset
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
    stats = None
    failed = 0
    data = run_method_using_mysqldb(method, dbs, conf, http.not_found)
    if data == http.not_found:
        print "Error running sql method."
        failed = failed + 1
    return data, failed

def aggregate(dbs, confs, method, strategy, **kw):
    stats = None
    failed = 0
    for conf in confs:
        conf.update(kw)
        data = run_method_using_mysqldb(method, dbs, conf, http.not_found)
        if data == http.not_found:
            print "Can't aggregate because of missing data."
            failed = failed + 1
        else:
            if stats is None:
                stats = data
            else:
                stats = merge(stats, data, strategy = strategy)
    return stats, failed

def collect(dbs, confs, method, strategy, **kw):
    results = []
    for conf in confs:
        conf.update(kw)
        data = run_method_using_mysqldb(method, dbs, conf, http.not_found)
        if data == http.not_found:
            print "Can't collect because of missing data."
        else:
            for line in data:
                results.append(strategy(conf, line))
    return results
    
def merge(d1, d2, strategy=lambda x,y:y):
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

    >>> d1
    {'a': 1, 'c': 3, 'b': 2}
    >>> merge(d1, d1)
    {'a': 1, 'c': 3, 'b': 2}
    >>> merge(d1, d1, lambda x,y: x+y)
    {'a': 2, 'c': 6, 'b': 4}

    """
    result = dict(d1)
    for k,v in d2.iteritems():
        if k in result:
            result[k] = strategy(result[k], v)
        else:
            result[k] = v
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

    def __init__(self, resolution, partition):
        self.resolution = resolution
        self.partition = partition

    def __call__(self, wrapped=None):
        if wrapped:
            # All levels at which there are statistics in the database 
            all_levels = [None, 'project', 'experiment', 'run', 'lane', 'read']
            # Only levels that are higher than the base resolution level are possible 
            possible_levels = all_levels[:all_levels.index(self.resolution)+1]
            for level in possible_levels:
                # Every statistic is known under a its name prefixed with the level
                # for which it is summarized
                if level == None:
                    # This is a top level resource, so no prefix needed
                    key = "%s" % wrapped.__name__
                else:
                    key = "%s_%s" % (level, wrapped.__name__)
                # Now store the new statistics level in the registry
                value = (wrapped, level, self.resolution, self.partition)
                stats_registry[key] = value

def get_dashboard_db(dbs, hgversion):
    if hgversion  == 'hg19':
        db = 'hg19_RNA_dashboard'
    elif hgversion  == 'hg18':
        db = 'hg18_RNA_dashboard'
    else:
        raise AttributeError
    dashboard_db = None
    # Search for the first matching db. This is done because we are not given a specific
    # project, and want to avoid hard-coding the project from which to take the db 
    for project_dbs in dbs.values():
        if project_dbs.has_key(db):
            dashboard_db = project_dbs[db]
            break
    return dashboard_db