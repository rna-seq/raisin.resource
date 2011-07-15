"""Summary statistics related to mapping"""

from utils import register_resource
from utils import aggregate


@register_resource(resolution="read", partition=False)
def mapping_summary(dbs, confs):
    chart = {}

    stats, failed = aggregate(dbs, confs['configurations'], _mapping_summary, lambda x, y: x + y)

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

    chart['table_data'] = _percentage_mapping_summary(stats, average_by)
    return chart


def _mapping_summary(dbs, conf):
    sql = """
select
    totalReads,
    uniqueReads,
    mappedReads
from
    %(projectid)s_%(runid)s_merged_mapping
where
    LaneName = '%(readid)s'""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    if rows:
        total, unique, mapped = rows[0]
        data = {'total': total,
                'unique': unique,
                'multimapped': mapped - unique,
                'unmapped': total - mapped,
               }
    else:
        data = None
    return data


def _percentage_mapping_summary(data, average_by):
    result = []
    if data is None:
        result.append(("Uniquely Mapped Reads", None, None))
        result.append(("Multi-Mapped Reads", None, None))
        result.append(("Unmapped Reads", None, None))
    else:
        unique = float(data['unique']) / average_by
        multimapped = float(data['multimapped']) / average_by
        unmapped = float(data['unmapped']) / average_by
        total = float(data['total']) / average_by
        result.append(("Uniquely Mapped Reads", int(unique), unique / total * 100.0))
        result.append(("Multi-Mapped Reads", int(multimapped), multimapped / total * 100.0))
        result.append(("Unmapped Reads", int(unmapped), unmapped / total * 100.0))
    return result


@register_resource(resolution="read", partition=True)
def merged_mapped_reads(dbs, confs):
    return mapped_reads(dbs, confs, 'merged_mapping')


@register_resource(resolution="read", partition=True)
def genome_mapped_reads(dbs, confs):
    return mapped_reads(dbs, confs, 'genome_mapping')


@register_resource(resolution="read", partition=True)
def junction_mapped_reads(dbs, confs):
    return mapped_reads(dbs, confs, 'junctions_mapping')


@register_resource(resolution="read", partition=True)
def split_mapped_reads(dbs, confs):
    return mapped_reads(dbs, confs, 'split_mapping')


def mapped_reads(dbs, confs, tableid):
    chart = {}
    chart['table_description'] = [(confs['level']['title'], 'string'),
                                  ('Total Reads',  'number'),
                                  ('Mapped Reads', 'number'),
                                  ('Unique Reads', 'number'),
                                  ('1:0:0 Reads',  'number'),
                                 ]
    result = []
    for partition, partition_confs in confs['configurations'].items():
        result.append(_mapped_reads(dbs, partition_confs, partition, tableid))
    result.sort()
    chart['table_data'] = result
    return chart


def _mapped_reads(dbs, confs, partition, tableid):
    stats, failed = aggregate(dbs, confs, _raw_mapped_reads, lambda x, y: x + y, tableid=tableid)

    average_by = len(confs) - failed

    if average_by == 0:
        return [partition, None, None, None, None]

    totalReads = float(stats['totalReads']) / average_by
    mappedReads = float(stats['mappedReads']) / average_by
    uniqueReads = float(stats['uniqueReads']) / average_by
    onezerozerouniqueReads = float(stats['100uniqueReads']) / average_by
    return [partition, totalReads, mappedReads, uniqueReads, onezerozerouniqueReads]


def _raw_mapped_reads(dbs, conf):
    sql = """
select
    totalReads,
    mappedReads,
    uniqueReads,
    100uniqueReads
from
    %(projectid)s_%(runid)s_%(tableid)s
where
     LaneName = '%(readid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    if rows:
        data = {'totalReads':     rows[0][0],
                'mappedReads':    rows[0][1],
                'uniqueReads':    rows[0][2],
                '100uniqueReads': rows[0][3],
               }
    else:
        data = {}
    return data
