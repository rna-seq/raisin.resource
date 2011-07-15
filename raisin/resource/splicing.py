"""Summary statistics for splicing"""

from utils import register_resource
from utils import aggregate
from utils import run


@register_resource(resolution="run", partition=False)
def splicing_summary(dbs, confs):
    chart = {}

    def adding(x, y):
        z = {'detected': x['detected'] + y['detected']}
        if x['total'] is None:
            z['total'] = None
        else:
            z['total'] = x['total'] + y['total']
        return z

    stats, failed = aggregate(dbs, confs['configurations'], _splicing_summary, adding)

    average_by = len(confs['configurations']) - failed

    if average_by == 0:
        resolution_info = ''
    elif average_by == 1:
        resolution_info = 'For one set of %ss' % confs['resolution']['id']
    else:
        resolution_info = 'Average over %s %ss' % (average_by, confs['resolution']['id'])

    chart['table_description'] = [(resolution_info, 'string'),
                                  ('Total', 'number'),
                                  ('Percent', 'number'),
                                 ]

    chart['table_data'] = _percentage_splicing_summary(stats, average_by)
    return chart


def _splicing_summary(dbs, conf):
    # Add a line for the totals
    sql = """
select junc_type,
       detected,
       total
from %(projectid)s_%(runid)s_splicing_summary""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    stats = {}
    for junc_type, detected, total in rows:
        stats[junc_type] = {'detected': detected, 'total': total}
    return stats


def _percentage_splicing_summary(data, average_by):
    result = []
    if data is None:
        result.append(('Known Junctions', None, None))
        result.append(('Novel Junctions from Annotated Exons', None, None))
        result.append(('Novel Junctions from Unannotated Exons', None, None))
    else:
        for key, value in data.items():
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
    chart = {}
    description = [('Percent', 'number'), ]
    partition_keys = confs['configurations'].keys()
    partition_keys.sort()
    for partition in partition_keys:
        description.append((partition, 'number'))
    chart['table_description'] = description

    coords = []
    partition_length = len(partition_keys)
    for partition in partition_keys:
        index = partition_keys.index(partition)
        for partition_conf in confs['configurations'][partition]:
            stats, failed = run(dbs, _exon_inclusion_profile, partition_conf)
            if not failed:
                for x, y in stats:
                    coords.append((index, x, y))

    result = []
    for index, x, y in coords:
        found = False
        for r in result:
            if r[0] == x and r[index + 1] == None:
                r[index + 1] = y
                found = True
                break
        if found:
            pass
        else:
            line = [None] * partition_length
            line[index] = y
            result.append([x] + line)

    if result:
        chart['table_data'] = result
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]

    return chart


def _exon_inclusion_profile(dbs, conf):
    sql = """
select incl_percent,
       support
from %(projectid)s_%(runid)s_inclusion_dist
where LaneName = "%(laneid)s"
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


@register_resource(resolution="run", partition=False)
def reads_supporting_exon_inclusions(dbs, confs, dumper=None):
    chart = {}
    chart['table_description'] = [('chr',                  'string'),
                                  ('start',                'number'),
                                  ('end',                  'number'),
                                  ('Exonic',               'number'),
                                  ('Inclusion Junctions',  'number'),
                                  ('Exclusion Junctions',  'number'),
                                  ('Inclusion Percentage', 'number'),
                                  ('Run Id',               'string'),
                                  ('Lane Id',              'string'),
                                 ]

    if dumper is None:
        pass
    else:
        dumper.writeheader(chart['table_description'])

    result = []
    for conf in confs['configurations']:
        if dumper is None:
            stats, failed = run(dbs, _top_reads_supporting_exon_inclusions, conf)
        else:
            stats, failed = run(dbs, _all_reads_supporting_exon_inclusions, conf)
        if not failed:
            for row in stats:
                line = row[:-1] + (conf['runid'], row[-1])
                if dumper is None:
                    result.append(line)
                else:
                    dumper.writerow(line)

    if dumper is None:
        pass
    else:
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
    %(projectid)s_%(runid)s_exon_inclusion_reads""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return list(rows)


def _top_reads_supporting_exon_inclusions(dbs, conf):
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
    %(projectid)s_%(runid)s_exon_inclusion_reads
order by
    JuncInc desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return list(rows)
