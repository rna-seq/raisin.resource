from utils import register_resource
from utils import collect

@register_resource(resolution="run", partition=False)
def novel_junctions_from_annotated_exons(dbs, confs, dumper=None):
    chart = {}
    chart['table_description'] = [('chr',     'string'), 
                                  ('start',   'number'), 
                                  ('end',     'number'),     
                                  ('# Reads', 'number'), 
                                  ('Run Id',  'string'),
                                  ('Lane Id', 'string'),
                                 ]

    if dumper is None:
        pass
    else:
        dumper.writeheader(chart['table_description'])
                                 
    def strategy(conf, row):
        return ( row[:-1] + (conf['runid'], row[-1]) )
    if dumper is None:
        stats = collect(dbs, confs['configurations'], _top_novel_junctions_from_annotated_exons, strategy)
    else:
        stats = collect(dbs, confs['configurations'], _all_novel_junctions_from_annotated_exons, strategy)

    if dumper is None:
        pass
    else:
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
    sql = """
select chr,
       start, 
       end, 
       support,
       sample
from 
    %(projectid)s_%(runid)s_novel_junctions_summary;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def _top_novel_junctions_from_annotated_exons(dbs, conf):
    sql = """
select * from ( 
select chr,
       start, 
       end, 
       support,
       sample
from 
    %(projectid)s_%(runid)s_novel_junctions_summary 
order by
    support desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

@register_resource(resolution="run", partition=False)
def novel_junctions_from_unannotated_exons(dbs, confs, dumper=None):
    chart = {}
    chart['table_description'] = [('start chr', 'string'),
                                  ('end chr',   'string'),
                                  ('start',     'number'),
                                  ('end',       'number'),
                                  ('# Reads',   'number'),
                                  ('Run Id',    'string'),
                                  ('Lane Id',   'string'),
                                  ]

    if dumper is None:
        pass
    else:
        dumper.writeheader(chart['table_description'])

    def strategy(conf, row):
        return ( row[:-1] + (conf['runid'], row[-1]) )
    if dumper is None:
        stats = collect(dbs, confs['configurations'], _top_novel_junctions_from_unannotated_exons, strategy)
    else:
        stats = collect(dbs, confs['configurations'], _all_novel_junctions_from_unannotated_exons, strategy)

    if dumper is None:
        pass
    else:
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
    sql = """
select
    start_chr, 
    end_chr, 
    start, 
    end, 
    number,
    filename
from
    %(projectid)s_%(runid)s_split_mapping_breakdown 
where
    type != 'close';""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
    
def _top_novel_junctions_from_unannotated_exons(dbs, conf):
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
    %(projectid)s_%(runid)s_split_mapping_breakdown 
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
