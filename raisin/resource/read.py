from utils import register_resource
from utils import aggregate

@register_resource(resolution="read", partition=False)
def read_summary(dbs, confs):
    chart = {}

    stats, failed = aggregate(dbs, confs['configurations'], _read_summary, lambda x, y: x+y)

    average_by = len(confs['configurations']) - failed
    
    if average_by == 0:
        resolution_info = ''
    elif average_by == 1:
        resolution_info = 'For one set of %ss' % confs['resolution']['id'] 
    else:
        resolution_info = 'Average over %s sets of %ss' % (average_by, confs['resolution']['id'])

    chart['table_description'] = [(resolution_info, 'string'), 
                                  ('Total', 'number'), 
                                  ('Percent', 'number'),
                                 ]

    chart['table_data'] = _percentage_read_summary(stats, average_by)
    return chart

def _read_summary(dbs, conf):
    # Add a line for the totals
    sql = """
select 
    TotalReads, 
    NoAmbiguousBases, 
    AmbiguousBases, 
    UniqueReads 
from
    %(projectid)s_%(runid)s_read_stats
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    data = {'total':rows[0][0],
            'unambiguous': rows[0][1],
            'ambiguous': rows[0][2],
            'unique': rows[0][3],
           }
    return data

def _percentage_read_summary(data, average_by):
    result = []
    if data is None:
        result.append( ('Unique Reads', None, None) )
        result.append( ('Unambiguous Reads', None, None) )
        result.append( ('Ambiguous Reads', None, None) )
    else:
        unique = float(data['unique']) / average_by
        unambiguous = float(data['unambiguous']) / average_by
        ambiguous = float(data['ambiguous']) / average_by
        total = float(data['total']) / average_by
        result.append( ("Unique Reads", int(unique), unique / total * 100.0))
        result.append( ("Unambiguous Reads", int(unambiguous), unambiguous / total * 100.0))
        result.append( ("Ambiguous Reads", int(ambiguous), ambiguous / total * 100.0))
    return result

@register_resource(resolution="read", partition=True)
def reads_containing_ambiguous_nucleotides(dbs, confs):
    chart = {}
    chart['table_description'] = [(confs['level']['title'], 'string'), 
                                  ('Reads containing ambiguous nucleotides', 'number'),
                                 ]
    result = []
    for partition_id, partition_confs in confs['configurations'].items():
        result.append(_partition_reads_containing_ambiguous_nucleotides(dbs, partition_confs, partition_id))
    result.sort()
    chart['table_data'] = result
    return chart
    
def _partition_reads_containing_ambiguous_nucleotides(dbs, confs, partition_id):
    stats, failed = aggregate(dbs, confs, _reads_containing_ambiguous_nucleotides, lambda x, y: x+y)

    if len(confs) - failed == 0:
        percent = None
    else:
        reads_containing_ambiguous_nucleotides = float(stats['reads_containing_ambiguous_nucleotides']) 
        total_number_of_reads = float(stats['total_number_of_reads']) 
        percent =  reads_containing_ambiguous_nucleotides / total_number_of_reads * 100.0

    return [partition_id, percent]

def _reads_containing_ambiguous_nucleotides(dbs, conf):
    sql = """
select 
    TotalReads, 
    AmbiguousBases
from
    %(projectid)s_%(runid)s_read_stats
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    data = {'total_number_of_reads':rows[0][0],
            'reads_containing_ambiguous_nucleotides': rows[0][1],
           }
    return data

@register_resource(resolution="read", partition=True)
def reads_containing_only_unambiguous_nucleotides(dbs, confs):
    chart = {}
    chart['table_description'] = [(confs['level']['title'], 'string'), 
                                  ('Reads containing only unambiguous nucleotides', 'number'),
                                 ]
    result = []
    for partition_id, partition_confs in confs['configurations'].items():
        result.append(_partition_reads_containing_only_unambiguous_nucleotides(dbs, partition_confs, partition_id))
    result.sort()
    chart['table_data'] = result
    return chart
    
def _partition_reads_containing_only_unambiguous_nucleotides(dbs, confs, partition_id):
    stats, failed = aggregate(dbs, confs, _reads_containing_only_unambiguous_nucleotides, lambda x, y: x+y)

    if len(confs) - failed == 0:
        percent = None
    else:
        reads_containing_only_unambiguous_nucleotides = float(stats['reads_containing_only_unambiguous_nucleotides']) 
        total_number_of_reads = float(stats['total_number_of_reads']) 
        percent =  reads_containing_only_unambiguous_nucleotides / total_number_of_reads * 100.0

    return [partition_id, percent]

def _reads_containing_only_unambiguous_nucleotides(dbs, conf):
    sql = """
select 
    TotalReads, 
    NoAmbiguousBases 
from
    %(projectid)s_%(runid)s_read_stats
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    data = {'total_number_of_reads':rows[0][0],
            'reads_containing_only_unambiguous_nucleotides': rows[0][1],
           }
    return data

@register_resource(resolution="read", partition=True)
def average_percentage_of_unique_reads(dbs, confs):
    chart = {}
    chart['table_description'] = [(confs['level']['title'], 'string'), 
                                  ('Unique Reads', 'number'),
                                 ]
    result = []
    for partition_id, partition_confs in confs['configurations'].items():
        result.append(_partition_average_percentage_of_unique_reads(dbs, partition_confs, partition_id))
    result.sort()
    chart['table_data'] = result
    return chart
    
def _partition_average_percentage_of_unique_reads(dbs, confs, partition_id):
    stats, failed = aggregate(dbs, confs, _average_percentage_of_unique_reads, lambda x, y: x+y)

    average_by = len(confs) - failed
    
    if average_by == 0:
        unique = None
    else:
        unique_reads = float(stats['unique_reads']) 
        total_number_of_reads = float(stats['total_number_of_reads']) 
        percent =  unique_reads / total_number_of_reads * 100.0

    return [partition_id,  percent]

def _average_percentage_of_unique_reads(dbs, conf):
    sql = """
select 
    TotalReads, 
    UniqueReads 
from
    %(projectid)s_%(runid)s_read_stats
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    data = {'total_number_of_reads':rows[0][0],
            'unique_reads': rows[0][1],
           }
    return data

@register_resource(resolution="read", partition=True)
def total_ambiguous_and_unambiguous_reads(dbs, confs):
    chart = {}
    chart['table_description'] = [('', 'string'), 
                                  ('Total', 'number'), 
                                  ('Unambiguous', 'number'),
                                  ('Ambiguous', 'number'),
                                 ]
    total = [0, 0, 0, ]
    result = []
    for partition_id, partition_confs in confs['configurations'].items():
        line = _partition_total_ambiguous_and_unambiguous_reads(dbs, partition_confs, partition_id)
        result.append(line)
        if not line[0] is None:
            total[0] = total[0] + line[1]
            total[1] = total[1] + line[2]
            total[2] = total[2] + line[3]
    result.sort()
    result.append(['Total'] + total)
    chart['table_data'] = result
    return chart

def _partition_total_ambiguous_and_unambiguous_reads(dbs, confs, partition_id):
    stats, failed = aggregate(dbs, confs, _total_ambiguous_and_unambiguous_reads, lambda x, y: x+y)

    if failed:
        unambiguous = None
        ambiguous = None
        total = None
    else:
        unambiguous = float(stats['unambiguous'])
        ambiguous = float(stats['ambiguous'])
        total = float(stats['total'])

    return [partition_id, total, unambiguous, ambiguous]
                       
def _total_ambiguous_and_unambiguous_reads(dbs, conf):
    # Add a line for the totals
    sql = """
select 
    TotalReads, 
    NoAmbiguousBases, 
    AmbiguousBases
from
    %(projectid)s_%(runid)s_read_stats
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    data = {'total':rows[0][0],
            'unambiguous': rows[0][1],
            'ambiguous': rows[0][2],
           }
    return data

@register_resource(resolution="read", partition=True)
def average_and_average_unique_reads(dbs, confs):
    chart = {}
    chart['table_description'] = [('', 'string'), 
                                  ('Average number of reads', 'number'), 
                                  ('Average number of unique reads', 'number'),
                                 ]
    result = []
    for partition_id, partition_confs in confs['configurations'].items():
        line = _partition_average_and_average_unique_reads(dbs, partition_confs, partition_id)
        result.append(line)
    result.sort()
    chart['table_data'] = result
    return chart

def _partition_average_and_average_unique_reads(dbs, confs, partition_id):
    stats, failed = aggregate(dbs, confs, _average_and_average_unique_reads, lambda x, y: x+y)

    average_by = len(confs) - failed
    
    if average_by == 0:
        unique = None
        total = None
    else:
        unique = float(stats['unique']) / average_by
        total = float(stats['total']) / average_by

    return [partition_id, total, unique]
                       
def _average_and_average_unique_reads(dbs, conf):
    # Add a line for the totals
    sql = """
select 
    TotalReads,
    UniqueReads
from
    %(projectid)s_%(runid)s_read_stats
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    data = {'total':rows[0][0],
            'unique': rows[0][1],
           }
    return data

@register_resource(resolution="read", partition=True)
def quality_score_by_position(dbs, confs):
    chart = {}
    description = [('Number', 'number'), ]
    partition_keys = confs['configurations'].keys()
    partition_keys.sort()
    for partition in partition_keys:
        description.append((partition, 'number'))
    chart['table_description'] = description

    result = []
    partition_length = len(partition_keys)
    for partition in partition_keys:
        partition_index = partition_keys.index(partition)
        for partition_conf in confs['configurations'][partition]:
            for position, mean in _quality_score_by_position(dbs, partition_conf):
                found = False
                for r in result:
                    if r[0] == position and r[partition_index+1] == None:
                        r[partition_index+1] = mean
                        found = True
                        break
                if found:
                    pass
                else:
                    line = [None] * partition_length
                    line[partition_index] = mean
                    result.append([position] + line)
    
    chart['table_data'] = result
    return chart

def _quality_score_by_position(dbs, conf):
    chart = {}
    sql = """
select 
    position, 
    mean 
from 
    %(projectid)s_%(runid)s_qualitiespos 
where 
    LaneName = '%(readid)s'
order by 
    position
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

@register_resource(resolution="read", partition=True)
def ambiguous_bases_per_position(dbs, confs):
    chart = {}
    description = [('Number', 'number'), ]
    partition_keys = confs['configurations'].keys()
    partition_keys.sort()
    for partition in partition_keys:
        description.append((partition, 'number'))
    chart['table_description'] = description

    result = []
    partition_length = len(partition_keys)
    for partition in partition_keys:
        partition_index = partition_keys.index(partition)
        for partition_conf in confs['configurations'][partition]:
            for position, ambiguous in _ambiguous_bases_per_position(dbs, partition_conf):
                found = False
                for r in result:
                    if r[0] == position and r[partition_index+1] == None:
                        r[partition_index+1] = ambiguous
                        found = True
                        break
                if found:
                    pass
                else:
                    line = [None] * partition_length
                    line[partition_index] = ambiguous
                    result.append([position] + line)
 
    chart['table_data'] = result
    return chart

def _ambiguous_bases_per_position(dbs, conf):
    chart = {}
    sql = """
select 
    position, 
    ambiguous 
from 
    %(projectid)s_%(runid)s_ambiguous 
where 
    LaneName = '%(readid)s'
order by 
    position
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
