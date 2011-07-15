"""Summary statistics of expression"""

import math
import random
from utils import register_resource
from utils import aggregate
from utils import run


@register_resource(resolution="run", partition=False)
def expression_summary(dbs, confs):
    chart = {}

    stats, failed = aggregate(dbs, 
                              confs['configurations'], 
                              _expression_summary, 
                              lambda x, y: x + y)

    average_by = len(confs['configurations']) - failed

    if average_by == 1:
        label = ''
    if average_by == 0:
        label = 'For one %s' % confs['resolution']['id']
    else:
        label = 'Average over %s %ss' % (average_by, confs['resolution']['id'])

    chart['table_description'] = [(label, 'string'),
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
        result.append(('Genes', None, None))
        result.append(('Transcript', None, None))
        result.append(('Exons', None, None))
    else:
        total = float(data[('Genes', 'total')]) / average_by
        detected = float(data[('Genes', 'detected')]) / average_by
        percent = detected / total * 100.0
        result.append(('Genes', int(total), int(detected), percent))

        total = float(data[('Transcripts', 'total')]) / average_by
        detected = float(data[('Transcripts', 'detected')]) / average_by
        percent = detected / total * 100.0
        result.append(('Transcript', int(total), int(detected), percent))

        total = float(data[('Exons', 'total')]) / average_by
        detected = float(data[('Exons', 'detected')]) / average_by
        percent = detected / total * 100.0
        result.append(('Exons', int(total), int(detected), percent))
    return result


@register_resource(resolution="run", partition=False)
def detected_genes(dbs, confs):
    chart = {}

    def adding(x, y):
        return {'detected': x['detected'] + y['detected'],
                'biotype': x['biotype'],
                'reliability': x['reliability'],
                }

    stats, failed = aggregate(dbs, 
                              confs['configurations'], 
                              _detected_genes, 
                              strategy=adding)

    if stats is None:
        chart['table_description'] = ['Type']
        chart['table_data'] = [[None]]
    else:
        biotypes = set()
        reliabilities = set()
        runids = set()
        for runid, biotype, reliability in stats.keys():
            runids.add(runid)
            reliabilities.add(reliability)
            biotypes.add(biotype)
        runids = list(runids)
        runids.sort()
        reliabilities = list(reliabilities)
        reliabilities.sort()
        biotypes = list(biotypes)
        biotypes.sort()

        description = [('Type',            'string'), ]

        #c:[{v:'miRNA'},{v:'NOVEL'},{v:27}]

        for reliability in reliabilities:
            description.append((reliability, 'number'))

        description.append((confs['resolution']['title'], 'string'))

        chart['table_description'] = description

        results = []
        for runid in runids:
            for biotype in biotypes:
                row = [biotype]
                for reliability in reliabilities:
                    detected = stats.get((runid, biotype, reliability), None)
                    if detected is None:
                        row.append(None)
                    else:
                        row.append(int(detected['detected']))
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
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    cells = {}
    for row in rows:
        key = (conf['runid'], row[0], row[1])
        if key in cells:
            raise AttributeError
        else:
            cells[key] = {'biotype': row[0], 
                          'reliability': row[1], 
                          'detected': row[2]}
    return cells


@register_resource(resolution="lane", partition=True)
def gene_expression_profile(dbs, confs):
    chart = {}
    description = [('Number', 'number'), ]
    partition_keys = confs['configurations'].keys()
    partition_keys.sort()
    for partition in partition_keys:
        description.append((partition, 'number'))
    chart['table_description'] = description

    coords = {}

    datapoints = 0
    partition_length = len(partition_keys)
    for partition in partition_keys:
        index = partition_keys.index(partition)
        for partition_conf in confs['configurations'][partition]:
            stats, failed = run(dbs, _gene_expression_profile, partition_conf)
            if not failed:
                for x, y in stats:
                    datapoints = datapoints + 1
                    if y in coords:
                        coords[y].append((index, x, y))
                    else:
                        coords[y] = [(index, x, y)]

    sample = []
    if confs['level']['id'] in ['lane', 'run'] or datapoints < 4000:
        # no random sampling
        for y, values in coords.items():
            sample = sample + values
    else:
        # There are a lot of entries for x between 1 and 10
        # This population can be sampled in order to reduce the dataset
        for y, values in coords.items():
            # take a random sample of items if the dataset is too big
            if len(values) > 1:
                # Divide the number of values by the logaritm to get a sample
                # size that reflects the size of the population
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


def _gene_expression_profile(dbs, conf):
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
    The selection process gives each lane a chance to contribute using randomization strategy.
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

    top_genes = {}
    for conf in confs['configurations']:
        key = (conf['runid'], conf['laneid'])
        top_genes[key] = _top_gene_expression_levels(dbs, conf)
        # Reverse. Items will be popped starting with most highly expressed.
        top_genes[key].reverse()

    # Compose the random seed from the laneids
    random.seed(tuple([c['laneid'] for c in confs['configurations']]))

    # These keys can be used to select from the top genes
    runid_laneid_keys = top_genes.keys()

    genes = []
    while len(genes) < 100:
        # Take out a random runid and laneid to choose the next gene candidate
        runid, laneid = random.choice(runid_laneid_keys)
        try:
            gene = top_genes[(runid, laneid)].pop()
        except IndexError:
            # This lane does not provide any more values
            continue
        if not gene in genes:
            genes.append(gene)

    print genes

    columns = [('Gene Name', 'string'), ]
    # Assemble the columns consisting of the gene names
    for gene in genes:
        columns.append((gene, 'number'))
    chart['table_description'] = columns

    result = []
    for conf in confs['configurations']:
        selected = _selected_gene_expression_levels(dbs, conf, genes)
        ordered = []
        for gene in genes:
            ordered.append(selected.get(gene, None))
        result.append(["%(runid)s %(laneid)s" % conf] + ordered)

    chart['table_data'] = result
    return chart


def _top_gene_expression_levels(dbs, conf):
    # Get the Top 100 genes for a lane
    sql = """
select
    gene_id
from
    %(projectid)s_%(runid)s_gene_RPKM
where
    LaneName = '%(laneid)s'
order by
    RPKM desc
limit 100""" % conf
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    return [row[0] for row in rows]


def _selected_gene_expression_levels(dbs, conf, genes):
    # For each gene, we need the value for all the lanes
    sql = """
select
    gene_id,
    RPKM
from
    %(projectid)s_%(runid)s_gene_RPKM
where
    LaneName = '%(laneid)s'""" % conf
    sql = """%s
and
    gene_id in ('%s')""" % (sql, "','".join(genes))
    cursor = dbs[conf['projectid']]['RNAseqPipeline'].query(sql)
    rows = cursor.fetchall()
    cursor.close()
    result = {}
    for row in rows:
        result[row[0]] = row[1]
    return result


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
