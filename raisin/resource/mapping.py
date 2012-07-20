"""Summary statistics related to mapping"""

from utils import register_resource
from utils import aggregate
from utils import collect
from raisin.mysqldb import run_method_using_mysqldb
from restish import http


@register_resource(resolution="lane", partition=False)
def read_distribution(dbs, confs):
    chart = {}
    chart['table_description'] = [
                                  ('Replicate', 'string'),
                                  ('Lane', 'string'),
                                  ('Start', 'number'),
                                  ('Transcript Length', 'number'), 
                                  ('Read Coverage', 'number'), 
                                 ]

    stats = []
    for conf in confs['configurations']:
        data = run_method_using_mysqldb(_read_distribution, dbs, conf, http.not_found)
        if data == http.not_found:
            print "Can't collect because of missing data."
        else:
            lines = []
            for line in data:
                lines.append((conf['replicateid'], line[0], line[1], line[2], line[3]))
            stats.extend(lines)

    if stats:
        chart['table_data'] = stats
    else:
        chart['table_data'] = [['', '', 0, 0, 0]]

    return chart


def _read_distribution(dbs, conf):
    """Query the database for the read distribution
      Table: Down_GFD1_read_dist_transcripts

      (('length_cat', 'varchar(50)', 'NO', 'MUL', None, ''),
       ('start', 'smallint(5) unsigned', 'NO', '', None, ''),
       ('position', 'tinyint(3) unsigned', 'NO', '', None, ''),
       ('hits', 'mediumint(8) unsigned', 'NO', '', None, ''),
       ('LaneName', 'varchar(50)', 'NO', 'MUL', None, ''))
             
      (('6000_8999', 6000, 0, 5433, 'GFD-1'),
       ('6000_8999', 6000, 1, 57575, 'GFD-1'),
       ('6000_8999', 6000, 2, 117959, 'GFD-1'),
       ('6000_8999', 6000, 3, 141873, 'GFD-1'),
       ('6000_8999', 6000, 4, 228211, 'GFD-1'),
      ('9000_n', 9000, 96, 152595, 'GFD-2'),
      ('9000_n', 9000, 97, 158966, 'GFD-2'),
      ('9000_n', 9000, 98, 134037, 'GFD-2'),
      ('9000_n', 9000, 99, 90585, 'GFD-2'),
      ('9000_n', 9000, 100, 17966, 'GFD-2'),
       ('100_999', 100, 98, 31299, 'GFD-2'),
       ('100_999', 100, 99, 15882, 'GFD-2'),
       ('100_999', 100, 100, 2682, 'GFD-2'),
       ('1_99', 1, 97, 5, 'GFD-2'),
       ('1_99', 1, 98, 14, 'GFD-2'),
       ('1_99', 1, 99, 5, 'GFD-2'))
       ('All', 0, 98, 1058910, 'GFD-2'),
       ('All', 0, 99, 589849, 'GFD-2'),
       ('All', 0, 100, 71349, 'GFD-2'))    
    """
    sql = """
select LaneName,
       start, 
       position, 
       hits 
from %(projectid)s_%(replicateid)s_read_dist_transcripts 
where
    LaneName = '%(laneid)s'
order by start, 
         position""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

@register_resource(resolution="read", partition=False)
def mapping_summary(dbs, confs):
    """Return an overview of the results after mapping"""
    chart = {}

    stats, failed = aggregate(dbs,
                              confs['configurations'],
                              _mapping_summary,
                              lambda x, y: x + y)

    average_by = len(confs['configurations']) - failed

    if average_by == 0:
        label = ''
    elif average_by == 1:
        label = 'For one set of %ss' % confs['resolution']['id']
    else:
        label = 'Average over %s sets of %ss' % (average_by,
                                                 confs['resolution']['id'])

    chart['table_description'] = [(label,     'string'),
                                  ('Total',   'number'),
                                  ('Percent', 'number'),
                                 ]

    chart['table_data'] = _percentage_mapping_summary(stats, average_by)
    return chart


def _mapping_summary(dbs, conf):
    """Query the database for the mapping statistics per read"""
    sql = """
select
    totalReads,
    uniqueReads,
    mappedReads
from
    %(projectid)s_%(replicateid)s_merged_mapping
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
    """Average the mapping statistics and calculate the percentages"""
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

        result.append(("Uniquely Mapped Reads",
                       int(unique),
                       unique / total * 100.0))
        result.append(("Multi-Mapped Reads",
                       int(multimapped),
                       multimapped / total * 100.0))
        result.append(("Unmapped Reads",
                       int(unmapped),
                       unmapped / total * 100.0))
    return result


@register_resource(resolution="read", partition=True)
def merged_mapped_reads(dbs, confs):
    """Summary of all the reads that were mapped"""
    return mapped_reads(dbs, confs, 'merged_mapping')


@register_resource(resolution="read", partition=True)
def genome_mapped_reads(dbs, confs):
    """Summary of the reads mapping to the genome"""
    return mapped_reads(dbs, confs, 'genome_mapping')


@register_resource(resolution="read", partition=True)
def junction_mapped_reads(dbs, confs):
    """Summary of the reads mapping to the junctions library"""
    return mapped_reads(dbs, confs, 'junctions_mapping')


@register_resource(resolution="read", partition=True)
def split_mapped_reads(dbs, confs):
    """Summary of those reads split-mapped to the genome"""
    return mapped_reads(dbs, confs, 'split_mapping')


def mapped_reads(dbs, confs, tableid):
    """Calculate partitioned read mappings using different SQL tables"""
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
    """Calculate read mappings using different SQL tables"""
    stats, failed = aggregate(dbs,
                              confs,
                              _raw_mapped_reads,
                              lambda x, y: x + y,
                              tableid=tableid)

    average_by = len(confs) - failed

    if average_by == 0:
        return [partition, None, None, None, None]

    total = float(stats['totalReads']) / average_by
    mapped = float(stats['mappedReads']) / average_by
    unique = float(stats['uniqueReads']) / average_by
    onezerozero = float(stats['100uniqueReads']) / average_by
    return [partition, total, mapped, unique, onezerozero]


def _raw_mapped_reads(dbs, conf):
    """Query the database for mapped reads using different SQL tables"""
    sql = """
select
    totalReads,
    mappedReads,
    uniqueReads,
    100uniqueReads
from
    %(projectid)s_%(replicateid)s_%(tableid)s
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
