"""Root object dispatching to restish resources"""

import pickle
import logging
from gvizapi import gviz_api
from gvizapi.gviz_api import DataTableException
try:
    import simplejson as json
except ImportError:
    try:
        import json  # NOQA
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
from utils import remove_chars

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
        return Resource(key, **kwargs), segments

    @resource.child('experiments')
    def experiments(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'experiments'
        return Resource(key, **kwargs), segments

    @resource.child('replicates_configurations')
    def replicates_configurations(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'replicates_configurations'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}')
    def project_info(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_info'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiments')
    def project_experiments(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiments'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/replicates')
    def project_replicates(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_replicates'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/replicates')
    def experiment_replicates(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'experiment_replicates'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiments/table')
    def project_experimentstable(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experimentstable'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/{parameter_list}/{parameter_values}')
    def project_experiment_subset(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/pending/{parameter_list}/{parameter_values}')
    def project_experiment_subset_pending(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset_pending'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/selection/{parameter_list}/{parameter_values}')
    def project_experiment_subset_selection(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset_selection'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiment/subset/start/{parameter_list}')
    def project_experiment_subset_start(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experiment_subset_start'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/downloads')
    def project_downloads(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_downloads'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/accessions')
    def project_accessions(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_accessions'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/experiments/tableraw')
    def project_experimentstableraw(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'project_experimentstableraw'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/technologies')
    def rnadashboard_technologies(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_technologies'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/rna_fractions')
    def rnadashboard_rna_fractions(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_rna_fractions'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/localizations')
    def rnadashboard_localizations(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_localizations'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}/files')
    def rnadashboard_files(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_files'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/rnadashboard/{hgversion}')
    def rnadashboard(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/results')
    def rnadashboard_results(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_results'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/accessions')
    def rnadashboard_accessions(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_accessions'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/rnadashboard/{hgversion}/replicates')
    def rnadashboard_replicates(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'rnadashboard_replicates'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/replicate/{replicateid}')
    def replicate_info(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'replicate_info'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}')
    def experiment_info(self, request, segments, **kwargs):
        """Define resource child"""
        key = 'experiment_info'
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/{parameter_list}/{parameter_values}/statistics/{stattype}/{statid}')
    def experiment_statistics(self, request, segments, **kwargs):
        """Define resource child"""
        key = kwargs['statid']
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/replicate/{replicateid}/statistics/{stattype}/{statid}')
    def replicate_statistics(self, request, segments, **kwargs):
        """Define resource child"""
        key = kwargs['statid']
        return Resource(key, **kwargs), segments

    @resource.child('project/{projectid}/replicate/{replicateid}/lane/{laneid}/statistics/{stattype}/{statid}')
    def lane_statistics(self, request, segments, **kwargs):
        """Define resource child"""
        key = kwargs['statid']
        return Resource(key, **kwargs), segments


class Resource(resource.Resource):
    """Resource that can be returned using different content types"""

    def __init__(self, key, **kwargs):
        """Keep all information about the resource"""
        default = (None, None, None, None)
        method, level, resolution, partition = STATS_REGISTRY.get(key, default)
        self.dbs = {}
        self.method = method
        self.level = level
        self.resolution = resolution
        self.partition = partition

        # Check the values passed in through the URL to avoid SQL injection
        for key, value in kwargs.items():
            if key == 'parameter_list':
                if not remove_chars(value, '-_.').isalnum():
                    raise AttributeError
            elif key == 'parameter_values':
                if not remove_chars(value, '-_.').isalnum():
                    raise AttributeError
            elif key == 'replicateid':
                if not remove_chars(value, '_').isalnum():
                    raise AttributeError
            elif key == 'laneid':
                if not remove_chars(value, '_.').isalnum():
                    raise AttributeError
            elif key == 'projectid':
                if not remove_chars(value, '_').isalnum():
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
