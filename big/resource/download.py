import datetime
from decimal import Decimal
from utils import register_resource
from utils import stats_registry

folder = "/tmp/%s/%s"

class ProjectStatisticsDumper:
    def __init__(self, confs, id):
        if confs['partition']:
            projectid = confs['configurations'].keys()[0]
        else:
            projectid = confs['configurations'][0]['projectid']
        self.file = open(folder % (projectid, id + '.csv'), 'w')
        
    def writeheader(self, table_description):
        self.types = []
        headers = []
        for header, type in table_description:
            headers.append('"%s"' % header.replace('"', '""'))
            self.types.append(type)
        self.file.write(', '.join(headers) + '\n')

    def format(self, value, value_type):
        if value is None:
            return "null"
            
        if value_type == "boolean":
            if value:
                return "true"
            return "false"

        if value_type == 'number':
            if isinstance(value, (int, long, float, Decimal)):
                return str(value)
            else:
                raise AttributeError
        elif value_type == "string":
            if isinstance(value, tuple):
                raise AttributeError
            return '"%s"' % value.replace('"', '""')
        raise AttributeError
        
    def writerow(self, row):
        formatted  = []
        for index in range(0, len(self.types)):
            formatted.append(self.format(row[index], self.types[index]))
        self.file.write(', '.join(formatted) + '\n')
            
    def close(self):
        self.file.close()
    
@register_resource(resolution="lane", partition=False)
def gene_expression_rpkm(dbs, confs):
    dumper=ProjectStatisticsDumper(confs, "gene_expression_rpkm")
    stats_registry['project_top_genes'][0](dbs, confs, dumper=dumper)
    return None
    
@register_resource(resolution="lane", partition=False)
def transcript_expression_rpkm(dbs, confs):
    dumper=ProjectStatisticsDumper(confs, "transcript_expression_rpkm")
    stats_registry['project_top_transcripts'][0](dbs, confs, dumper=dumper)
    return None

@register_resource(resolution="lane", partition=False)
def exon_expression_rpkm(dbs, confs):
    dumper=ProjectStatisticsDumper(confs, "exon_expression_rpkm")
    stats_registry['project_top_exons'][0](dbs, confs, dumper=dumper)
    return None

@register_resource(resolution="run", partition=False)
def exon_number_of_reads(dbs, confs):
    dumper=ProjectStatisticsDumper(confs, "exon_number_of_reads")
    stats_registry['project_reads_supporting_exon_inclusions'][0](dbs, confs, dumper=dumper)
    return None

@register_resource(resolution="run", partition=False)
def novel_junctions_from_annotated_exons_number_of_reads(dbs, confs):
    dumper=ProjectStatisticsDumper(confs, "novel_junctions_from_annotated_exons_number_of_reads")
    stats_registry['project_novel_junctions_from_annotated_exons'][0](dbs, confs, dumper=dumper)
    return None

@register_resource(resolution="run", partition=False)
def novel_junctions_from_unannotated_exons_number_of_reads(dbs, confs):
    dumper=ProjectStatisticsDumper(confs, "novel_junctions_from_unannotated_exons_number_of_reads")
    stats_registry['project_novel_junctions_from_unannotated_exons'][0](dbs, confs, dumper=dumper)
    return None
