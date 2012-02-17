"""Root object dispatching to restish resources"""

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

STATS_REGISTRY = {}

import experiment
import replicate
import project
import read
import mapping
import expression
import splicing
import discovery

from raisin.mysqldb import run_method_using_mysqldb
from utils import get_configurations
from utils import to_cfg


log = logging.getLogger(__name__)  # pylint: disable-msg=C0103


class Root(resource.Resource):
    """Root object for the resources."""

    # pylint: disable-msg=W0613
    # The request variable is not used in any of the methods on Root

    # pylint: disable-msg=C0301
    # XXX There are long lines that could be removed with a refactoring

    # pylint: disable-msg=R0201
    # Methods could be functions

    # pylint: disable-msg=R0904
    # Too many methods.
    @resource.child('projects')
    def projects(self, request, segments, **kwargs):
        """Define resource child"""
        key = "project_projects"
        cachefilebase = "projects"
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('experiments')
    def experiments(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'experiments'
        cachefilebase = "experiments"
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('replicates_configurations')
    def replicates_configurations(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'replicates_configurations'
        cachefilebase = 'replicates_configurations'
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}')
    def project_info(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_info'
        cachefilebase = "project/%(projectid)s/info" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiments')
    def project_experiments(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiments'
        cachefilebase = "project/%(projectid)s/experiments" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/replicates')
    def project_replicates(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_replicates'
        cachefilebase = "project/%(projectid)s/replicates" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/replicates')
    def experiment_replicates(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'experiment_replicates'
        cachefilebase = "project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/replicates" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiments/table')
    def project_experimentstable(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experimentstable'
        cachefilebase = "project/%(projectid)s/experiments/table" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/{parameter_list}/{parameter_values}')
    def project_experiment_subset(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset'
        cachefilebase = "project/%(projectid)s/experiment/subset/%(parameter_list)s/%(parameter_values)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/pending/{parameter_list}/{parameter_values}')
    def project_experiment_subset_pending(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset_pending'
        cachefilebase = "project/%(projectid)s/experiment/subset/pending/%(parameter_list)s/%(parameter_values)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/selection/{parameter_list}/{parameter_values}')
    def project_experiment_subset_selection(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset_selection'
        cachefilebase = "project/%(projectid)s/experiment/subset/selection/%(parameter_list)s/%(parameter_values)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/start/{parameter_list}')
    def project_experiment_subset_start(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset_start'
        cachefilebase = "project/%(projectid)s/experiment/subset/start/%(parameter_list)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/downloads')
    def project_downloads(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_downloads'
        cachefilebase = "project/%(projectid)s/downloads" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/accessions')
    def project_accessions(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_accessions'
        cachefilebase = "project/%(projectid)s/accessions" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/experiments/tableraw')
    def project_experimentstableraw(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experimentstableraw'
        cachefilebase = "project/%(projectid)s/experiments/tableraw" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/technologies')
    def rnadashboard_technologies(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_technologies'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/technologies" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/rna_fractions')
    def rnadashboard_rna_fractions(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_rna_fractions'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/rna_fractions" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/localizations')
    def rnadashboard_localizations(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_localizations'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/localizations" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/files')
    def rnadashboard_files(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_files'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/files" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}')
    def rnadashboard(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/results')
    def rnadashboard_results(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_results'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/results" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/accessions')
    def rnadashboard_accessions(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_accessions'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/accessions" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/replicates')
    def rnadashboard_replicates(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_replicates'
        cachefilebase = "project/%(projectid)s/rnadashboard/%(hgversion)s/replicates" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/replicate/{replicateid}')
    def replicate_info(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'replicate_info'
        cachefilebase = "project/%(projectid)s/replicate/%(replicateid)s/info" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}')
    def experiment_info(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'experiment_info'
        cachefilebase = "project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/info" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/statistics/{stattype}/{statid}')
    def experiment_statistics(self, request, segments, **kwargs):
        """Define resource child"""
        key = kwargs['statid']
        cachefilebase = "project/%(projectid)s/%(parameter_list)s/%(parameter_values)s/statistics/%(stattype)s/%(statid)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/replicate/{replicateid}/statistics/{stattype}/{statid}')
    def replicate_statistics(self, request, segments, **kwargs):
        """Define resource child"""
        key = kwargs['statid']
        cachefilebase = "project/%(projectid)s/replicate/%(replicateid)s/statistics/%(stattype)s/%(statid)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments

    @resource.child('project/{projectid}/replicate/{replicateid}/lane/{laneid}/statistics/{stattype}/{statid}')
    def lane_statistics(self, request, segments, **kwargs):
        """Define resource child"""
        key = kwargs['statid']
        cachefilebase = "project/%(projectid)s/replicate/%(replicateid)s/lane/%(laneid)s/statistics/%(stattype)s/%(statid)s" % kwargs
        return Resource(key, cachefilebase, **kwargs), segments


class Resource(resource.Resource):
    """Resource that can be returned using different content types"""

    def __init__(self, key, cachefilebase, **kwargs):
        """Keep all information about the resource"""
        default = (None, None, None, None)
        method, level, resolution, partition = STATS_REGISTRY.get(key, default)
        self.dbs = {}
        self.method = method
        self.level = level
        self.resolution = resolution
        self.partition = partition
        self.cachefilebase = cachefilebase

        # Check the values passed in through the URL to avoid SQL injection
        for key, value in kwargs.items():
            if key == 'parameter_list':
                if not value.replace('-', '')\
                            .replace('_', '')\
                            .replace('.', '').isalnum():
                    raise AttributeError
            elif key == 'parameter_values':
                if not value.replace('-', '')\
                            .replace('_', '')\
                            .replace('.', '').isalnum():
                    raise AttributeError
            elif key == 'replicateid':
                if not value.replace('_', '').isalnum():
                    raise AttributeError
            elif key == 'laneid':
                if not value.replace('_', '')\
                            .replace('.', '').isalnum():
                    raise AttributeError
            elif key == 'projectid':
                if not value.replace('_', '').isalnum():
                    raise AttributeError
            elif key == 'statid':
                if not value in STATS_REGISTRY:
                    raise AttributeError(value)
            elif key == 'stattype':
                if not value in ('read',
                                 'mapping',
                                 'expression',
                                 'splicing',
                                 'discovery'):
                    raise AttributeError
            elif key == 'hgversion':
                if not value in ['hg18', 'hg19']:
                    raise AttributeError
            else:
                raise AttributeError
        log.info(kwargs)
        # The statid and stattype are only needed for routing, but are
        # superfluous for the method
        self.kwargs = kwargs.copy()
        if 'statid' in kwargs:
            del self.kwargs['statid']
        if 'stattype' in kwargs:
            del self.kwargs['stattype']

    @resource.GET()
    def show(self, request):
        """Return the resource representation in the requested content type"""
        if not self.method:
            # The method needs to be set at least
            return http.not_found([('Content-type', 'text/javascript')], '')

        # Inject the project specific project databases
        self.dbs = request.environ['dbs']

        # Get the pickle cache file if available
        cachefile = None
        if not request.environ['pickles_cache_path'] is None:
            print "Pickles Cache path is there"
            # Get the path to the pickles from the repoze.bfg ini file
            cachefile = os.path.join(request.environ['pickles_cache_path'],
                                           self.cachefilebase + '.pickle')

        # The data should change to something else if we can get something
        # from the cache now.
        data = None

        # First try getting data out of the cache if this is defined
        if not cachefile is None:
            if os.path.isfile(cachefile):
                print "Read pickle cache file", cachefile
                data = pickle.loads(open(cachefile, 'r').read())

        # If the data was not found, get it out of the databases if that
        # is defined
        if data is None:
            # Get the configurations for the given level of detail
            confs = get_configurations(request,
                                       self.level,
                                       self.resolution,
                                       self.partition,
                                       self.dbs,
                                       **self.kwargs)

            # Run the method containing the code to access the database
            data = run_method_using_mysqldb(self.method,
                                            self.dbs,
                                            confs,
                                            http.not_found)

            if data == http.not_found:
                # If the returned value is the marker http.not_found, we know
                # that something related to MySQL went wrong when the method
                # was called. The marker is used so that no internals of the
                # MySQL adapter need to be considered here.
                return http.not_found()
            else:
                # Only store data if the pickles cache is to be used
                if not cachefile is None:
                    print "Write pickle cache file", cachefile
                    # Store the data in the pickles cache
                    picklescachefile_folder = os.path.split(cachefile)[0]
                    # Create the directories on the way to where the file
                    # should be stored
                    if not os.path.exists(picklescachefile_folder):
                        os.makedirs(picklescachefile_folder)
                    pickle.dump(data, open(cachefile, 'w'))

        if data is None:
            log.warning("Method appears to be unimplemented: %s" % self.method)
            return http.not_found([('Content-type', 'text/javascript')], '')

        accept_header = request.headers.get('Accept', 'text/javascript')
        body = None

        # Different results are returned depending on whether this is a table
        if 'table_description' in data and 'table_data' in data:
            #print "Extract table info and return info"
            # This chart is using the google visualization library
            table = gviz_api.DataTable(data['table_description'])
            try:
                table.AppendData(data['table_data'])
            except DataTableException:
                print self.method
                raise
            if accept_header == 'text/plain':
                body = table.ToJSonResponse()
            elif accept_header == 'text/html':
                body = table.ToHtml()
            elif accept_header == 'text/x-cfg':
                body = to_cfg(data)
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
            accept_header = request.headers.get('Accept', None)
            if accept_header == 'text/x-python-pickled-dict':
                body = pickle.dumps(data)
            else:
                body = json.dumps(data)

        headers = [('Content-type', accept_header),
                   ('Content-Length', len(body))]
        return http.ok(headers, body)
