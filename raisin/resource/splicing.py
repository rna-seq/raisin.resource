"""Summary statistics for splicing"""

from utils import register_resource
from utils import aggregate
from utils import run


@register_resource(resolution="replicate", partition=False)
def splicing_summary(dbs, confs):
    """Fetch splicing summary chart"""
    chart = {}

    def adding(x, y):
        """Add the values. Ignoring the absence of a value for the total."""
        z = {'detected': x['detected'] + y['detected']}
        if x['total'] is None:
            z['total'] = None
        else:
            z['total'] = x['total'] + y['total']
        return z

    stats, failed = aggregate(dbs,
                              confs['configurations'],
                              _splicing_summary,
                              adding)

    average_by = len(confs['configurations']) - failed

    if average_by == 0:
        label = ''
    elif average_by == 1:
        label = 'For one set of %ss' % confs['resolution']['id']
    else:
        label = 'Average over %s %ss' % (average_by, confs['resolution']['id'])

    chart['table_description'] = [(label,     'string'),
                                  ('Total',   'number'),
                                  ('Percent', 'number'),
                                 ]

    chart['table_data'] = _percentage_splicing_summary(stats, average_by)
    return chart


def _splicing_summary(dbs, conf):
    """Fetch splicing_summary results from db"""
    # Add a line for the totals
    sql = """
select junc_type,
       detected,
       total
from %(projectid)s_%(replicateid)s_splicing_summary""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    stats = {}
    for junc_type, detected, total in rows:
        stats[junc_type] = {'detected': detected, 'total': total}
    return stats


def _percentage_splicing_summary(data, average_by):
    """Add percentages to splicing_summary"""
    result = []
    if data is None:
        result.append(('Known Junctions', None, None))
        result.append(('Novel Junctions from Annotated Exons', None, None))
        result.append(('Novel Junctions from Unannotated Exons', None, None))
    else:
        for value in data.values():
            detected = float(value['detected']) / average_by
            if value['total'] is None:
                value['percent'] = None
            else:
                total = float(value['total']) / average_by
                value['percent'] = detected / total * 100.0
            if not detected is None:
                value['detected'] = detected

        result = []
        result.append(('Known Junctions',
                       int(data['Known']['detected']),
                       data['Known']['percent']))
        result.append(('Novel Junctions from Annotated Exons',
                       int(data['Novel']['detected']),
                       data['Novel']['percent']))
        result.append(('Novel Junctions from Unannotated Exons',
                       int(data['Unannotated']['detected']),
                       data['Unannotated']['percent']))
    return result


@register_resource(resolution="lane", partition=True)
def exon_inclusion_profile(dbs, confs):
    """Fetch exon_inclusion_profile chart"""
    chart = {}
    chart['table_description'] = [('Percent', 'number'), ]
    partition_keys = confs['configurations'].keys()
    partition_keys.sort()
    for partition in partition_keys:
        chart['table_description'].append((partition, 'number'))

    coords = []
    for partition in partition_keys:
        index = partition_keys.index(partition)
        for partition_conf in confs['configurations'][partition]:
            stats, success = run(dbs, _exon_inclusion_profile, partition_conf)
            if success:
                for x, y in stats:
                    coords.append((index, x, y))

    partition_length = len(partition_keys)

    result = []
    for index, x, y in coords:
        found = False
        for item in result:
            if item[0] == x and item[index + 1] == None:
                item[index + 1] = y
                found = True
                break
        if not found:
            line = [None] * partition_length
            line[index] = y
            result.append([x] + line)

    if result:
        chart['table_data'] = result
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]

    return chart


def _exon_inclusion_profile(dbs, conf):
    """Fetch exon_inclusion_profile results from database"""
    sql = """
select incl_percent,
       support
from %(projectid)s_%(replicateid)s_inclusion_dist
where LaneName = "%(laneid)s"
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


@register_resource(resolution="replicate", partition=False)
def reads_supporting_exon_inclusions(dbs, confs, dumper=None):
    """Fetch reads_supporting_exon_inclusions chart"""
    chart = {}
    chart['table_description'] = [('chr',                  'string'),
                                  ('start',                'number'),
                                  ('end',                  'number'),
                                  ('Exonic',               'number'),
                                  ('Inclusion Junctions',  'number'),
                                  ('Exclusion Junctions',  'number'),
                                  ('Inclusion Percentage', 'number'),
                                  ('Replicate Id',         'string'),
                                  ('Lane Id',              'string'),
                                 ]

    if not dumper is None:
        dumper.writeheader(chart['table_description'])

    result = []
    for conf in confs['configurations']:
        if dumper is None:
            stats, success = run(dbs,
                                 _top_reads_supporting_exon_inclusions, conf)
        else:
            stats, success = run(dbs,
                                 _all_reads_supporting_exon_inclusions, conf)
        if success:
            for row in stats:
                line = row[:-1] + (conf['replicateid'], row[-1])
                if dumper is None:
                    result.append(line)
                else:
                    dumper.writerow(line)

    if not dumper is None:
        dumper.close()
        return

    if result:
        result = sorted(result, key=lambda row: row[4])
        result.reverse()
        chart['table_data'] = result[:20]
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]

    return chart


def _all_reads_supporting_exon_inclusions(dbs, conf):
    """Fetch all all_reads_supporting_exon_inclusions results from db"""
    sql = """
select chr,
       start,
       end,
       ExIncl,
       JuncInc,
       JuncExc,
       inc_rate * 100,
       sample_id
from
    %(projectid)s_%(replicateid)s_exon_inclusion_reads""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return list(rows)


def _top_reads_supporting_exon_inclusions(dbs, conf):
    """Fetch top 20 top_reads_supporting_exon_inclusions results from db"""
    sql = """
select * from (
select chr,
       start,
       end,
       ExIncl,
       JuncInc,
       JuncExc,
       inc_rate * 100,
       sample_id
from
    %(projectid)s_%(replicateid)s_exon_inclusion_reads
order by
    JuncInc desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return list(rows)
