# http://code.google.com/apis/visualization/documentation/reference.html
from decimal import Decimal
import math
import random
from utils import register_resource
from utils import aggregate
from utils import run

@register_resource(resolution="run", partition=False)
def expression_summary(dbs, confs):
    chart = {}

    stats, failed = aggregate(dbs, confs['configurations'], _expression_summary, lambda x,y: x+y)

    average_by = len(confs['configurations']) - failed
    
    if average_by == 1:
        resolution_info = ''
    if average_by == 0:
        resolution_info = 'For one %s' % confs['resolution']['id']
    else:
        resolution_info = 'Average over %s %ss' % (average_by, confs['resolution']['id'])
    
    chart['table_description'] = [(resolution_info, 'string'), 
                                  ('Total',    'number'), 
                                  ('Detected', 'number'), 
                                  ('Percent',  'number'),
                                 ]

    chart['table_data'] = _percentage_expression_summary(stats, average_by)
    return chart

def _expression_summary(dbs, conf):
    # Down_GFD1_expression_summary
    sql = """
select type, 
       total, 
       detected
from %(projectid)s_%(runid)s_expression_summary""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result = {}
    for feature_type, total, detected in rows:
        result[(feature_type, 'total')] = total
        result[(feature_type, 'detected')] = detected
    return result

def _percentage_expression_summary(data, average_by):
    result = []
    if data is None:
        result.append( ('Genes', None, None) )
        result.append( ('Transcript', None, None) )
        result.append( ('Exons', None, None) )
    else:
        genes_detected = float(data[('Genes', 'detected')]) / average_by
        genes_total = float(data[('Genes', 'total')]) / average_by
        genes_percent = Decimal( "%.1f" % (genes_detected / genes_total * 100.0) ) 
        transcripts_detected = float(data[('Transcripts', 'detected')]) / average_by
        transcripts_total = float(data[('Transcripts', 'total')]) / average_by
        transcripts_percent = Decimal( "%.1f" % (transcripts_detected / transcripts_total * 100.0 ) )
        exons_detected = float(data[('Exons', 'detected')]) / average_by
        exons_total = float(data[('Exons', 'total')]) / average_by
        exons_percent = Decimal( "%.1f" % (exons_detected / exons_total * 100.0 ) )  
        result.append( ('Genes', genes_total, genes_detected, genes_percent) ) 
        result.append( ('Transcript', transcripts_total, transcripts_detected, transcripts_percent) )
        result.append( ('Exons', exons_total, exons_detected, exons_percent) )
    return result
    
@register_resource(resolution="run", partition=False)
def detected_genes(dbs, confs):
    chart = {}

    def adding(x, y):
        return {'detected': x['detected'] + y['detected'],
                'type': x['type'],
                'reliability': x['reliability'],
                }
                
    stats, failed = aggregate(dbs, confs['configurations'], _detected_genes, strategy=adding)
    
    if stats is None:
        chart['table_description'] = ['Type']
        chart['table_data'] = [[None]]
    else:
        types = set()
        reliabilities = set()
        runids = set()
        for runid, type, reliability in stats.keys():
            runids.add(runid)
            reliabilities.add(reliability)
            types.add(type)
        runids = list(runids)
        runids.sort()
        reliabilities = list(reliabilities)
        reliabilities.sort()
        types = list(types)
        types.sort()
        
        description = [('Type',            'string'),]
    
        #c:[{v:'miRNA'},{v:'NOVEL'},{v:27}]
    
        for reliability in reliabilities:
            description.append((reliability, 'number'))

        description.append((confs['resolution']['title'], 'string'))
        
        chart['table_description'] = description
    
        results = []
        for runid in runids:
            for type in types:
                row = [type]
                for reliability in reliabilities:
                    detected = stats.get((runid, type,reliability),None)
                    if detected is None:
                        row.append(None)
                    else:
                        row.append(detected['detected'])
                if row[1] or row[2]:
                    results.append(row + [runid])
        results.sort()
        chart['table_data'] = results
    return chart


def _detected_genes(dbs, conf):
    sql = """
select type, 
       reliability, 
       sum(detected) 
from %(projectid)s_%(runid)s_detected_genes
group by type, reliability;
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql )
    rows = cursor.fetchall()
    cursor.close()
    cells = {}        
    for row in rows:
        if cells.has_key((conf['runid'], row[0], row[1])):
            raise AttributeError
        else:
            cells[(conf['runid'], row[0], row[1])] = {'type':row[0], 'reliability':row[1], 'detected':row[2]}
    return cells

@register_resource(resolution="lane", partition=True)
def gene_expression_profile(dbs, confs):
    chart = {}
    description = [('Number', 'number'),]
    partition_keys = confs['configurations'].keys()
    partition_keys.sort()
    for partition in partition_keys:
        description.append((partition, 'number'))
    chart['table_description'] = description

    distribution = {}

    datapoints = 0
    all = []
    partition_length = len(partition_keys)
    for partition in partition_keys:
        index = partition_keys.index(partition)
        for partition_conf in confs['configurations'][partition]:
            stats, failed = run(dbs, _gene_expression_profile, partition_conf)
            if not failed:
                for x, y in stats:
                    datapoints = datapoints + 1
                    if distribution.has_key(y):
                        distribution[y].append( (index, x, y) )
                    else:
                        distribution[y] = [(index, x, y) ]

    sample = []
    if confs['level']['id'] in ['lane', 'run'] or datapoints < 4000:
        # no random sampling
        for y, values in distribution.items():
            sample = sample + values
    else:
        # There are a lot of entries for x between 1 and 10
        # This population can be sampled in order to reduce the dataset
        for y, values in distribution.items():
            # take a random sample of items if the dataset is too big
            if len(values) > 1:
                # Divide the number of values by the logaritm to get a sample size that reflects the 
                # size of the population
                # y:1 5677 -> 656
                # y:6 260 -> 46
                # y:136 10 -> 4
                size = int(len(values) / math.log(len(values)))
                if size <= len(values):
                    sample = sample + random.sample(values, size)
            else:
                sample = sample + values

    result = []
    for index, x, y in sample:
        y = int(y)
        found = False
        for r in result:
            if r[0] == x and r[index+1] == None:
                r[index+1] = y
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

def _gene_expression_profile(dbs, conf):
    chart = {}
    sql = """
select 
    rpkm, 
    support 
from 
    %(projectid)s_%(runid)s_gene_RPKM_dist 
where 
    LaneName = '%(laneid)s'
""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

@register_resource(resolution="lane", partition=False)
def gene_expression_levels(dbs, confs):
    """
    Implement a greedy strategy of getting just enough of the top rpkms from any lane to be 
    sure to be able to finish the selection of at least 100 genes.
    The selection process gives each lane a chance to contribute using a round robin strategy.
    Minimally, a lane may contribute 0 genes, which is the case when we have not found
    any gene from this lane after 100 rounds.
    Maximally, a lane can contribute 100 genes if all of the top genes come from one
    lane.
    On average, given a random distribution of top rpkms across the lanes, each lane is 
    given an equal chance of contributing a gene.
    In the worst case, the maximum amount of data points we need is 100*#lanes, which is only
    going to be the case when all lanes have the same genes and are found in alternating order.

    Now that the top genes are decided, get the top 100 values from the different lanes for
    these genes.
    """
    chart = {}

    lanes = [conf['laneid'] for conf in confs['configurations']]

    projectid = confs['configurations'][0]['projectid']
    runid = confs['configurations'][0]['runid']

    by_lane = {}
    for conf in confs['configurations']:
        lane = lane_gene_expression_levels(dbs, conf)
        # Just take out the names of the top genes
        lane['table_description'].reverse()
        lane['table_description'].pop()
        by_lane[conf['laneid']] = lane['table_description']
        
    genes = []
    lanes_to_poll = lanes * 100
    lanes_to_poll.reverse()
    result = []
    while len(genes) < 100:
        laneid = lanes_to_poll.pop()
        try:
            gene, ignore = by_lane[laneid].pop()
        except IndexError:
            # This lane does not provide any more values
            continue
        if not gene in genes:
            genes.append(gene)
        
    columns = [('Gene Name', 'string'),]
    # Assemble the columns consisting of the gene names
    for gene in genes:
        columns.append( (gene, 'number') )

    chart['table_description'] = columns

    lane_rows = {}
    for lane in lanes:
        lane_rows[lane] = [0] * 100
             
    # For each gene, we need the value for all the lanes
    sql = """
select gene_id, 
       RPKM, 
       LaneName 
from %s_%s_gene_RPKM 
where gene_id in ('%s')""" % (projectid, runid, "','".join(genes))
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()

    for row in rows:
        if row[2] in lanes:
            # get the RPKM for all genes and lanes
            gene_id, RPKM, LaneName = row[0], row[1], row[2]
            # Now sort them into the right lane, taking into account the position
            # of the gene in the reference lane
            lane_rows[LaneName][genes.index(gene_id)] = Decimal( "%.0f" % RPKM)

    result = []
    # Put the rows into the table ordered by the lane name
    for lane in lanes:
        result.append([lane] + lane_rows[lane])

    chart['table_data'] = result
    return chart

def lane_gene_expression_levels(dbs, conf):
    chart = {}
        
    # Get the Top genes for all lanes limited by 100*#lanes
    sql = """
select gene_id,
       RPKM
from %(projectid)s_%(runid)s_gene_RPKM 
where LaneName = '%(laneid)s'
order by RPKM desc 
limit 100""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    # Assemble the list of genes
    rows = cursor.fetchall()
    cursor.close()

    columns = [('Gene Name', 'string'),]
    rpkms = []
    for gene_id, RPKM in rows:
        # Assemble the columns consisting of the gene names
        columns.append( (gene_id, 'number') )
        rpkms.append(Decimal( "%.0f" % RPKM))

    result = []
    result.append( [conf['laneid']] + rpkms )

    chart['table_description'] = columns
    chart['table_data'] = result
    return chart

@register_resource(resolution="lane", partition=False)
def top_genes(dbs, confs, dumper=None):
    chart = {}
    chart['table_description'] = [('Gene Id', 'string'), 
                                  ('Length', 'number'), 
                                  ('Strand (+/-)', 'string'),
                                  ('Locus', 'string'),
                                  ('# Exons', 'number'),
                                  ('# Transcripts', 'number'),
                                  ('Expression Value', 'number'),
                                  ('Run Id', 'string'),
                                  ('Lane Id', 'string'),
                                 ]

    if dumper is None:
        pass
    else:
        dumper.writeheader(chart['table_description'])
                                 
    result = []
    for conf in confs['configurations']:
        if dumper is None:
            stats, failed = run(dbs, _top_genes, conf)
        else:
            stats, failed = run(dbs, _all_genes, conf)
        if not failed:
            for row in stats:
                line = row + (conf['runid'], conf['laneid']) 
                if dumper is None:
                    result.append(line)
                else:
                    dumper.writerow(line)
    if result:
        if dumper is None:
            result = sorted(result, key=lambda row: row[6])
            result.reverse()
            chart['table_data'] = result[:20]        
        else:
            chart['table_data'] = result
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]
    return chart

def _all_genes(dbs, conf):
    sql = """
select gene_id, 
       length,
       strand,
       locus,
       no_exons,
       no_transcripts,
       %(laneid)s
from 
    %(projectid)s_%(runid)s_top_genes_expressed""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
    
def _top_genes(dbs, conf):
    sql = """
select * from ( 
select gene_id, 
       length,
       strand,
       locus,
       no_exons,
       no_transcripts,
       %(laneid)s
from 
    %(projectid)s_%(runid)s_top_genes_expressed
order by
    %(laneid)s desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

@register_resource(resolution="lane", partition=False)
def top_transcripts(dbs, confs, dumper=None):
    chart = {}
    chart['table_description'] = [('Gene Id', 'string'), 
                                  ('Length', 'number'), 
                                  ('Strand (+/-)', 'string'),
                                  ('Locus', 'string'),
                                  ('# Exons', 'number'),
                                  ('Expression Value', 'number'),
                                  ('Run Id', 'string'),
                                  ('Lane Id', 'string'),
               ]

    if dumper is None:
        pass
    else:
        dumper.writeheader(chart['table_description'])

    result = []
    for conf in confs['configurations']:
        if dumper is None:
            stats, failed = run(dbs, _top_transcripts, conf)
        else:
            stats, failed = run(dbs, _all_transcripts, conf)
        if not failed:
            for row in stats:
                line = row + (conf['runid'], conf['laneid']) 
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
        result = sorted(result, key=lambda row: row[5])
        result.reverse()
        chart['table_data'] = result[:20]        
    else:
        chart['table_data'] = [[None] * len(chart['table_description'])]
    return chart

def _all_transcripts(dbs, conf):
    sql = """
select transcript_id, 
       length,
       strand,
       locus,
       no_exons,
       %(laneid)s
from 
    %(projectid)s_%(runid)s_top_transcripts_expressed""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def _top_transcripts(dbs, conf):
    sql = """
select * from ( 
select transcript_id, 
       length,
       strand,
       locus,
       no_exons,
       %(laneid)s
from 
    %(projectid)s_%(runid)s_top_transcripts_expressed
order by
    %(laneid)s desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
    
@register_resource(resolution="lane", partition=False)
def top_exons(dbs, confs, dumper=None):
    chart = {}
    description = [('Exon Id', 'string'), 
                   ('Length', 'number'), 
                   ('Strand (+/-)', 'string'),
                   ('Locus', 'string'),
                   ('Expression Value', 'number'),
                   ('Run Id', 'string'),
                   ('Lane Id', 'string'),
                  ]
    chart['table_description'] = description

    if dumper is None:
        pass
    else:
        dumper.writeheader(chart['table_description'])

    result = []
    for conf in confs['configurations']:
        if dumper is None:
            stats, failed = run(dbs, _top_exons, conf)
        else:
            stats, failed = run(dbs, _all_exons, conf)
        if not failed:
            for row in stats:
                line = row + (conf['runid'], conf['laneid']) 
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

def _all_exons(dbs, conf):
    sql = """
select 
    exon_id,
    length,
    strand,
    locus,
    %(laneid)s
from 
    %(projectid)s_%(runid)s_top_exons_expressed""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def _top_exons(dbs, conf):
    sql = """
select * from ( 
select 
    exon_id,
    length,
    strand,
    locus,
    %(laneid)s
from 
    %(projectid)s_%(runid)s_top_exons_expressed
order by
    %(laneid)s desc
) x
limit 20;""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows
