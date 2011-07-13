import os.path
import pickle
import logging
from gvizapi import gviz_api
from gvizapi.gviz_api import DataTableException
try:
    import simplejson as json
except ImportError:
    try:
        import json
    except:
        print 'Unable to find json module!'
        raise

from restish import http, resource

stats_registry = {}

import experiment
import run
import project
import read
import mapping
import expression
import splicing
import discovery

from raisin.mysqldb import run_method_using_mysqldb
from utils import get_configurations
log = logging.getLogger(__name__)


class Root(resource.Resource):

    @resource.child('projects')
    def projects(self, request, segments, **kw):
        key = "project_projects"
        cachefilebase = "projects"
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('experiments')
    def experiments(self, request, segments, **kw):
        key = 'experiments'
        cachefilebase = "experiments"
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('experiments_configurations')
    def experiments_configurations(self, request, segments, **kw):
        key = 'experiments_configurations'
        cachefilebase = 'experiments_configurations'
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}')
    def project_info(self, request, segments, **kw):
        key = 'project_info'
        cachefilebase = "project/%(projectid)s/info" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/experiments')
    def project_experiments(self, request, segments, **kw):
        key = 'project_experiments'
        cachefilebase = "project/%(projectid)s/experiments" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/runs')
    def project_runs(self, request, segments, **kw):
        key = 'project_runs'
        cachefilebase = "project/%(projectid)s/runs" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/runs')
    def experiment_runs(self, request, segments, **kw):
        key = 'experiment_runs'
        cachefilebase = "project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/runs" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/experiments/table')
    def project_experimentstable(self, request, segments, **kw):
        key = 'project_experimentstable'
        cachefilebase = "project/%(projectid)s/experiments/table" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/experiment/subset/{parameter_list}/{parameter_values}')
    def project_experiment_subset(self, request, segments, **kw):
        key = 'project_experiment_subset'
        cachefilebase = "project/%(projectid)s/experiment/subset/%(parameter_list)s/%(parameter_values)s" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/experiment/subset/selection/{parameter_list}/{parameter_values}')
    def project_experiment_subset_selection(self, request, segments, **kw):
        key = 'project_experiment_subset_selection'
        cachefilebase = "project/%(projectid)s/experiment/subset/selection/%(parameter_list)s/%(parameter_values)s" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/downloads')
    def project_downloads(self, request, segments, **kw):
        key = 'project_downloads'
        cachefilebase = "project/%(projectid)s/downloads" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/experiments/tableraw')
    def project_experimentstableraw(self, request, segments, **kw):
        key = 'project_experimentstableraw'
        cachefilebase = "project/%(projectid)s/experiments/tableraw" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/technologies')
    def rnadashboard_technologies(self, request, segments, **kw):
        key = 'rnadashboard_technologies'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/technologies" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/rna_fractions')
    def rnadashboard_rna_fractions(self, request, segments, **kw):
        key = 'rnadashboard_rna_fractions'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/rna_fractions" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/compartments')
    def rnadashboard_compartments(self, request, segments, **kw):
        key = 'rnadashboard_compartments'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/compartments" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/files')
    def rnadashboard_files(self, request, segments, **kw):
        key = 'rnadashboard_files'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/files" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}')
    def rnadashboard(self, request, segments, **kw):
        key = 'rnadashboard'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/results')
    def rnadashboard_results(self, request, segments, **kw):
        key = 'rnadashboard_results'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/results" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/run/{runid}')
    def run_info(self, request, segments, **kw):
        key = 'run_info'
        cachefilebase = "project/%(projectid)s/run/%(runid)s/info" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}')
    def experiment_info(self, request, segments, **kw):
        key = 'experiment_info'
        cachefilebase = "project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/info" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/statistics/{stattype}/{statid}')
    def experiment_statistics(self, request, segments, **kw):
        key = kw['statid']
        cachefilebase = "project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/statistics/%(stattype)s/%(statid)s" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/run/{runid}/statistics/{stattype}/{statid}')
    def run_statistics(self, request, segments, **kw):
        key = kw['statid']
        cachefilebase = "project/%(projectid)s/run/%(runid)s/statistics/%(stattype)s/%(statid)s" % kw
        return Resource(key, cachefilebase, **kw), segments

    @resource.child('project/{projectid}/run/{runid}/lane/{laneid}/statistics/{stattype}/{statid}')
    def lane_statistics(self, request, segments, **kw):
        key = kw['statid']
        cachefilebase = "project/%(projectid)s/run/%(runid)s/lane/%(laneid)s/statistics/%(stattype)s/%(statid)s" % kw
        return Resource(key, cachefilebase, **kw), segments


class Resource(resource.Resource):

    def __init__(self, key, cachefilebase, **kw):
        method, level, resolution, partition = stats_registry.get(key, (None, None, None, None))
        self.dbs = {}
        self.method = method
        self.level = level
        self.resolution = resolution
        self.partition = partition
        self.cachefilebase = cachefilebase

        # Check the values passed in through the URL to avoid SQL injection
        for key, value in kw.items():
            if key == 'parameter_list':
                if not value.replace('-', '').replace('_', '').replace('.', '').isalnum():
                    raise AttributeError
            elif key == 'parameter_values':
                if not value.replace('-', '').replace('_', '').replace('.', '').isalnum():
                    raise AttributeError
            elif key == 'runid':
                if not value.replace('_', '').isalnum():
                    raise AttributeError
            elif key == 'laneid':
                if not value.replace('_', '').replace('.', '').isalnum():
                    raise AttributeError
            elif key == 'projectid':
                if not value.replace('_', '').isalnum():
                    raise AttributeError
            elif key == 'statid':
                if not value in stats_registry:
                    raise AttributeError(value)
            elif key == 'stattype':
                if not value in ('read', 'mapping', 'expression', 'splicing', 'discovery'):
                    raise AttributeError
            elif key == 'hgversion':
                if not value in ['hg18', 'hg19']:
                    raise AttributeError
            else:
                raise AttributeError
        log.info(kw)
        # The statid and stattype are only needed for routing, but are superfluous for the method
        self.kw = kw.copy()
        if 'statid' in kw:
            del self.kw['statid']
        if 'stattype' in kw:
            del self.kw['stattype']

    @resource.GET()
    def show(self, request):
        if not self.method:
            # The method needs to be set at least
            return http.not_found([('Content-type', 'text/javascript')], '')

        # Inject the project specific project databases
        self.dbs = request.environ['dbs']

        # Get the pickle cache file if available
        picklecachefile = None
        if not request.environ['pickles_cache_path'] is None:
            print "Pickles Cache path is there"
            # Get the path to the pickles from the repoze.bfg ini file
            picklecachefile = os.path.join(request.environ['pickles_cache_path'],
                                           self.cachefilebase + '.pickle')

        # The data should change to something else if we can get something from the cache now.
        data = None

        # First try getting data out of the cache if this is defined
        if not picklecachefile is None:
            if os.path.isfile(picklecachefile):
                print "Read pickle cache file", picklecachefile
                data = pickle.loads(open(picklecachefile, 'r').read())

        # If the data was not found, get it out of the databases if that is defined
        if data is None:
            # Get the configurations for the given level of detail
            confs = get_configurations(request, self.level, self.resolution, self.partition, self.dbs, **self.kw)

            # Run the method containing the code to access the database
            data = run_method_using_mysqldb(self.method, self.dbs, confs, http.not_found)

            if data == http.not_found:
                # If the returned value is the marker http.not_found, we know that something related
                # to MySQL went wrong when the method was called.
                # The marker is used so that no internals of MySQL need to be considered here.
                return http.not_found()
            else:
                # Only store data if the pickles cache is to be used
                if not picklecachefile is None:
                    print "Write pickle cache file", picklecachefile
                    # Store the data in the pickles cache
                    picklescachefile_folder = os.path.split(picklecachefile)[0]
                    # Create the directories on the way to where the file should be stored
                    if not os.path.exists(picklescachefile_folder):
                        os.makedirs(picklescachefile_folder)
                    pickle.dump(data, open(picklecachefile, 'w'))

        if data is None:
            print "The method is not yet implemented, it has returned a None value"
            # The method needs to be set at least
            return http.not_found([('Content-type', 'text/javascript')], '')

        accept_header = request.headers.get('Accept', 'text/javascript')
        body = None

        # Different results are returned depending on whether this is a table
        if 'table_description' in data and 'table_data' in data:
            #print "Extract table info and return info"
            # This chart is using the google visualization library
            table = gviz_api.DataTable(data['table_description'])
            table.AppendData(data['table_data'])
            if accept_header == 'text/plain':
                body = table.ToJSonResponse()
            elif accept_header == 'text/html':
                body = table.ToHtml()
            elif accept_header == 'text/csv':
                body = table.ToCsv()
            elif accept_header == 'text/tab-separated-values':
                body = table.ToCsv(separator="\t")
            elif accept_header == 'text/x-python-pickled-dict':
                body = pickle.dumps(data)
            else:
                try:
                    body = table.ToJSon()
                except DataTableException:
                    print self.method
                    print data['table_description']
                    print data['table_data']
                    raise
                except:
                    raise
        else:
            if request.headers.get('Accept', None) == 'text/x-python-pickled-dict':
                body = pickle.dumps(data)
            else:
                body = json.dumps(data)

        headers = [('Content-type', accept_header), ('Content-Length', len(body))]
        return http.ok(headers, body)
