"""Summary statistics for discovery of novel junctions"""

from utils import register_resource
from utils import collect


@register_resource(resolution="replicate", partition=False)
def novel_junctions_from_annotated_exons(dbs, confs, dumper=None):
    """List novel junctions from annotated exons."""
    chart = {}
    chart['table_description'] = [('chr',          'string'),
                                  ('start',        'number'),
                                  ('end',          'number'),
                                  ('# Reads',      'number'),
                                  ('Replicate Id', 'string'),
                                  ('Lane Id',      'string'),
                                 ]

    if not dumper is None:
        dumper.writeheader(chart['table_description'])

    def strategy(conf, row):
        """Insert replicateid"""
        return (row[: -1] + (conf['replicateid'], row[-1]))

    if dumper is None:
        stats = collect(dbs,
                        confs['configurations'],
                        _top_novel_junctions_from_annotated_exons,
                        strategy)
    else:
        stats = collect(dbs,
                        confs['configurations'],
                        _all_novel_junctions_from_annotated_exons,
                        strategy)

    if not dumper is None:
        for line in stats:
            dumper.writerow(line)
        dumper.close()
        return

    if stats:
        stats = sorted(stats, key=lambda row: row[3])
        stats.reverse()
        chart['table_data'] = stats[:20]
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]
    return chart


def _all_novel_junctions_from_annotated_exons(dbs, conf):
    """Query the database for all results."""
    sql = """
select chr,
       start,
       end,
       support,
       sample
from
    %(projectid)s_%(replicateid)s_novel_junctions_summary;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _top_novel_junctions_from_annotated_exons(dbs, conf):
    """Query the database for the top 20."""
    sql = """
select * from (
select chr,
       start,
       end,
       support,
       sample
from
    %(projectid)s_%(replicateid)s_novel_junctions_summary
order by
    support desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


@register_resource(resolution="replicate", partition=False)
def novel_junctions_from_unannotated_exons(dbs, confs, dumper=None):
    """List novel junctions from unannotated exons."""
    chart = {}
    chart['table_description'] = [('start chr',     'string'),
                                  ('end chr',       'string'),
                                  ('start',         'number'),
                                  ('end',           'number'),
                                  ('# Reads',       'number'),
                                  ('Replicate Id',  'string'),
                                  ('Lane Id',       'string'),
                                  ]

    if not dumper is None:
        dumper.writeheader(chart['table_description'])

    def strategy(conf, row):
        """Insert replicateid"""
        return (row[:-1] + (conf['replicateid'], row[-1]))

    if dumper is None:
        stats = collect(dbs,
                        confs['configurations'],
                        _top_novel_junctions_from_unannotated_exons,
                        strategy)
    else:
        stats = collect(dbs,
                        confs['configurations'],
                        _all_novel_junctions_from_unannotated_exons,
                        strategy)

    if not dumper is None:
        for line in stats:
            dumper.writerow(line)
        dumper.close()
        return

    if stats:
        stats = sorted(stats, key=lambda row: row[4])
        stats.reverse()
        chart['table_data'] = stats[:20]
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]
    return chart


def _all_novel_junctions_from_unannotated_exons(dbs, conf):
    """Query the database for all results."""
    sql = """
select
    start_chr,
    end_chr,
    start,
    end,
    number,
    filename
from
    %(projectid)s_%(replicateid)s_split_mapping_breakdown
where
    type != 'close';""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _top_novel_junctions_from_unannotated_exons(dbs, conf):
    """Query the database for the top 20 results."""
    sql = """
select * from (
select
    start_chr,
    end_chr,
    start,
    end,
    number,
    filename
from
    %(projectid)s_%(replicateid)s_split_mapping_breakdown
where
    type != 'close'
order by
    number desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
