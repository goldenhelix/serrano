import functools
from datetime import datetime
from django.core.cache import cache
from django.http import HttpResponse, StreamingHttpResponse
from restlib2.params import Parametizer
from restlib2.resources import Resource
from avocado.models import DataContext, DataView, DataQuery, DataField, DataConcept
from ceviche.models import ModelSeries, ModelVersion
from serrano.conf import settings
from django.contrib.auth import authenticate, login
from ..tokens import get_request_token
from .. import cors
import sys
import urllib
import json
from django.db.models.sql.constants import JoinInfo


__all__ = ('BaseResource', 'ThrottledResource')

SAFE_METHODS = ('GET', 'HEAD', 'OPTIONS')

def get_count(queryset):
    query = queryset.query
    has_where = bool(query.where.children)
    is_project = any(t.startswith('p_') for t in query.tables)
    if not has_where and is_project:
        model_name = [t for t in query.tables if t.startswith('p_')][0]
        model_name = model_name.rstrip('_entity')
        model_name = model_name.rstrip('_matrix')
        model_version = ModelVersion.objects.filter(model_name=model_name)[0]
        count = model_version.aux_data["variant_count"]
    else:
        count = queryset.count()

    return count

def get_alias_map(model_name, current_alias_map):

    alias_map = {}
    if model_name=='projectsample':
        alias_map['sample_record_schema'] = JoinInfo(table_name='sample_record_schema', rhs_alias='sample_record_schema', join_type=None, lhs_alias=None, lhs_join_col=None, \
                                                            rhs_join_col=None, nullable=False)
    else:
        alias_map[model_name] = JoinInfo(table_name=model_name, rhs_alias=model_name, join_type=None, lhs_alias=None, lhs_join_col=None, \
                                                            rhs_join_col=None, nullable=False) 

        # make sure to include joins on sample tables
        for table in current_alias_map:
            if not table==model_name:
                alias_map[table] = current_alias_map[table]

    return alias_map

def get_url(value, template):
    url = ""
    if template=='AUTO_RSID_OR_COSM':
        if value.startswith('COSM'):
            value = value.split('COSM')[1]
            template = 'http://grch37-cancer.sanger.ac.uk/cosmic/mutation/overview?id=$$'
        else:
            template = 'http://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=$$'
                    
    if '$$' in template:
        tsplit = template.split('$$')
        url = tsplit[0] + str(value) + tsplit[1]

    return url

# Returns a map of values to urls
def url_from_template(value, template):
    # try to load lists if they are encoded as strings
    if isinstance(value, basestring):
        if ';' in value:
            value = value.split(';')
        else:
            try:
                value = json.loads(value.replace("'", '"'))
            except ValueError:
                pass

        # don't urlify n/a
        if value and type(value)==list or isinstance(value, basestring):
            if value and 'n/a' in value or 'None' in value:
                value = None

    urls = {}
    if  template and value:
        

        if type(value)==list:
            for v in value:
                if v is None:
                    continue
                if type(v)==list:
                    v = str(v)
                urls[v] = get_url(v, template)
        else:
            urls[value] = get_url(value, template)


    return urls
            


def prune_view_columns(view, model_version_id):
    # get required concept objects
    order = {c.id:c.order for c in DataConcept.objects.filter(published=True)}
    for key in order:
        if not order[key]:
            order[key] = sys.maxint

    if 'columns' not in view.json:
        concepts = [c.id for c in DataConcept.objects.filter(published=True, is_default=True)]
        view.json['columns'] = []
        for concept_id in concepts:
            view.json["columns"].append(concept_id)

    column_list = []
    for c in view.json['columns']:
        concept = DataConcept.objects.get(id=c)
        if concept.model_version_id==model_version_id:
            column_list.append(c)

    view.json['columns'] = column_list

    return view   

def extract_model_version(request):
    
    url_components = []

    if 'PATH_INFO' in request.META: url_components = request.META['PATH_INFO'].split('/')
    if not ('query' in url_components or 'results' in url_components or 'workspace' in url_components): 
        if 'HTTP_REFERER' in request.META:
            url_components = request.META['HTTP_REFERER'].split('/')
        else:
            return {}

    url_components = [urllib.unquote(s) for s in url_components]
 
    series_version = int(url_components[len(url_components)-3])
    series_id = int(url_components[len(url_components)-4].split('-')[0])
    model_type, record_type = url_components[len(url_components)-5].split('_')
    model_version = ModelVersion.objects.get(series__id=series_id, version=series_version, series__model_type=model_type, series__record_type=record_type)
    record_type = model_version.series.record_type
        
    model_version_data = model_version.__dict__   
    model_version_data['model_type']  = model_version.series.model_type
    model_version_data['record_type'] = record_type
    model_version_data['series_name'] = model_version.series.name
    model_version_data['keys'] = model_version.series.aux_data.get("keys", [])

    return model_version_data


def page_type(request):
    if 'PATH_INFO' in request.META: url_components = request.META['PATH_INFO'].split('/')
    if not ('query' in url_components or 'results' in url_components or 'workspace' in url_components): url_components = request.META['HTTP_REFERER'].split('/')
    if 'query' in url_components: return 'query'
    elif 'results' in url_components: return 'results'
    else: return 'other'

def map_concepts_to_models():
    query = ("select avocado_datafield.id, conceptfield.concept_id, avocado_datafield.model_name from (select * from "
    "avocado_dataconcept inner join avocado_dataconceptfield on avocado_dataconcept.id=avocado_dataconceptfield.concept_id) "
    "as conceptfield left outer join avocado_datafield on avocado_datafield.id=conceptfield.field_id;")
    results = DataConcept.objects.raw(query)

    return {r.concept_id : r.model_name for r in results} 


def _get_request_object(request, attrs=None, klass=None, key=None):
    """Resolves the appropriate object for use from the request.

    This applies only to DataView or DataContext objects.
    """
    # Attempt to derive the `attrs` from the request
    if attrs is None:
        if request.method == 'POST':
            attrs = request.data.get(key)
        elif request.method == 'GET':
            attrs = request.GET.get(key)

    # If the `attrs` still could not be resolved, try to get the view or
    # context from the query data if it exists within the request.
    if attrs is None:
        request_data = None

        # Try to read the query data from the request
        if request.method == 'POST':
            request_data = request.data.get('query')
        elif request.method == 'GET':
            request_data = request.GET.get('query')

        # If query data was found in the request, then attempt to create a
        # DataQuery object from it.
        if request_data:
            query = get_request_query(request, attrs=request_data.get('query'))

            # Now that the DataQuery object is built, read the appropriate
            # attribute from it, returning None if the attribute wasn't found.
            # Since `context` and `view` are the keys used in get_request_view
            # and get_request_context respectively, we can use the key directly
            # to access the context and view properties of the DataQuery model.
            key_object = getattr(query, key, None)

            # If the property exists and is not None, then read the json from
            # the object as both DataContext and DataView objects will have a
            # json property. This json will be used as the attributes to
            # construct or lookup the klass object going forward. Otherwise,
            # `attrs` will still be None and we are no worse off than we were
            # before attempting to create and read the query.
            if key_object:
                attrs = key_object.json

    # Use attrs that were supplied or derived from the request.
    # This provides support for one-off queries via POST or GET.
    if isinstance(attrs, (list, dict)):
        return klass(json=attrs)

    kwargs = {}

    # If an authenticated user made the request, filter by the user or
    # fallback to an active session key.
    if getattr(request, 'user', None) and request.user.is_authenticated():
        kwargs['user'] = request.user
    else:
        # If no session has been created, this is a cookie-less user agent
        # which is most likely a bot or a non-browser client (e.g. cURL).
        if request.session.session_key is None:
            return klass()
        kwargs['session_key'] = request.session.session_key

    if klass==DataContext or klass==DataView:
        model_version = extract_model_version(request)
        kwargs['model_version_id'] = model_version['id']

    # Assume it is a primary key and fallback to the sesssion
    try:
        kwargs['pk'] = int(attrs)
    except (ValueError, TypeError):
        kwargs['session'] = True

    try:
        # Check that multiple DataViews or DataContexts are not returned
        # If there are more than one, return the most recent
        return klass.objects.filter(**kwargs).latest('modified')
    except klass.DoesNotExist:
        pass

    # Fallback to an instance based off the default template if one exists
    instance = klass()
    default = klass.objects.get_default_template()
    if default:
        instance.json = default.json
    return instance


# Partially applied functions for DataView and DataContext. These functions
# only require the request object and an optional `attrs` dict
get_request_view = functools.partial(
    _get_request_object, klass=DataView, key='view')
get_request_context = functools.partial(
    _get_request_object, klass=DataContext, key='context')


def get_request_query(request, attrs=None):
    """
    Resolves the appropriate DataQuery object for use from the request.
    """
    # Attempt to derive the `attrs` from the request
    if attrs is None:
        if request.method == 'POST':
            attrs = request.data.get('query')
        elif request.method == 'GET':
            attrs = request.GET.get('query')

    # If the `attrs` could not be derived from the request(meaning no query
    # was explicity defined), try to construct the query by deriving a context
    # and view from the request.
    if attrs is None:
        json = {}

        context = get_request_context(request)
        if context:
            json['context'] = context.json

        view = get_request_view(request)
        if view:
            json['view'] = view.json

        return DataQuery(json)

    # If `attrs` were derived or supplied then validate them and return a
    # DataQuery based off the `attrs`.
    if isinstance(attrs, dict):
        # We cannot simply validate and create a DataQuery based off the
        # `attrs` as they are now because the context and or view might not
        # contain json but might instead be a pk or some other value. Use the
        # internal helper methods to construct the context and view objects
        # and build the query from the json of those objects' json.
        json = {}

        context = get_request_context(request, attrs=attrs)
        if context:
            json['context'] = context.json
        view = get_request_view(request, attrs=attrs)
        if view:
            json['view'] = view.json

        DataQuery.validate(json)
        return DataQuery(json)

    kwargs = {}

    # If an authenticated user made the request, filter by the user or
    # fallback to an active session key.
    if getattr(request, 'user', None) and request.user.is_authenticated():
        kwargs['user'] = request.user
    else:
        # If not session has been created, this is a cookie-less user agent
        # which is most likely a bot or a non-browser client (e.g. cURL).
        if request.session.session_key is None:
            return DataQuery()
        kwargs['session_key'] = request.session.session_key

    # Assume it is a primary key and fallback to the sesssion
    try:
        kwargs['pk'] = int(attrs)
    except (ValueError, TypeError):
        kwargs['session'] = True

    try:
        return DataQuery.objects.get(**kwargs)
    except DataQuery.DoesNotExist:
        pass

    # Fallback to an instance based off the default template if one exists
    instance = DataQuery()
    default = DataQuery.objects.get_default_template()
    if default:
        instance.json = default.json
    return instance


class BaseResource(Resource):
    param_defaults = None

    parametizer = Parametizer

    def is_unauthorized(self, request, *args, **kwargs):
        user = getattr(request, 'user', None)

        # Attempt to authenticate if a token is present
        if not user or not user.is_authenticated():
            token = get_request_token(request)
            user = authenticate(token=token)

            if user:
                login(request, user)
            elif settings.AUTH_REQUIRED:
                return True

    def process_response(self, request, response):
        response = super(BaseResource, self).process_response(
            request, response)
        response = cors.patch_response(request, response, self.allowed_methods)
        return response

    def get_params(self, request):
        "Returns cleaned set of GET parameters."
        return self.parametizer().clean(request.GET, self.param_defaults)

    def get_context(self, request, attrs=None):
        "Returns a DataContext object based on `attrs` or the request."
        return get_request_context(request, attrs=attrs)

    def get_view(self, request, attrs=None):
        "Returns a DataView object based on `attrs` or the request."
        view = get_request_view(request, attrs=attrs)
        return view

    def get_query(self, request, attrs=None):
        "Returns a DataQuery object based on `attrs` or the request."
        return get_request_query(request, attrs=attrs)

    def dispatch(self, request, *args, **kwargs):
        # Process the request. This includes all the necessary checks prior to
        # actually interfacing with the resource itself.
        response = self.process_request(request, *args, **kwargs)

        if not isinstance(response, HttpResponse):
            # Attempt to process the request given the corresponding
            # `request.method` handler.
            method_handler = getattr(self, request.method.lower())
            response = method_handler(request, *args, **kwargs)

            if isinstance(response, StreamingHttpResponse):
                return response

            if not isinstance(response, HttpResponse):
                # If the return value of the handler is not a response, pass
                # the return value into the render method.
                response = self.render(request, response, args=args,
                                       kwargs=kwargs)

        # Process the response, check if the response is overridden and
        # use that instead.
        return self.process_response(request, response)

    @property
    def checks_for_orphans(self):
        return settings.CHECK_ORPHANED_FIELDS


class ThrottledResource(BaseResource):
    def __init__(self, **kwargs):
        if settings.RATE_LIMIT_COUNT:
            self.rate_limit_count = settings.RATE_LIMIT_COUNT

        if settings.RATE_LIMIT_SECONDS:
            self.rate_limit_seconds = settings.RATE_LIMIT_SECONDS

        self.auth_rate_limit_count = settings.AUTH_RATE_LIMIT_COUNT \
            or self.rate_limit_count

        self.auth_rate_limit_seconds = settings.AUTH_RATE_LIMIT_SECONDS \
            or self.rate_limit_seconds

        return super(ThrottledResource, self).__init__(**kwargs)

    def is_too_many_requests(self, request, *arg, **kwargs):
        limit_count = self.rate_limit_count
        limit_seconds = self.rate_limit_seconds

        # Check for an identifier for this request. First, try to use the
        # user id and then try the session key as a fallback. If this is an
        # authenticated request then we prepend an indicator to the request
        # id and use the authenticated limiter settings.
        if getattr(request, 'user', None) and request.user.is_authenticated():
            request_id = "auth:{0}".format(request.user.id)
            limit_count = self.auth_rate_limit_count
            limit_seconds = self.auth_rate_limit_seconds
        elif request.session.session_key:
            request_id = request.session.session_key
        else:
            # The only time we should reach this point is for
            # non-authenitcated, cookieless agents(bots). Simply return False
            # here and let other methods decide how to deal with the bot.
            return False

        # Construct the cache key from the request identifier and lookup
        # the current cached value for the key. The counts that are stored in
        # the cache are tuples where the 1st value is the request count for
        # the given time interval and the 2nd value is the start of the
        # time interval.
        cache_key = 'serrano:data_request:{0}'.format(request_id)
        current_count = cache.get(cache_key)

        # If there is nothing cached for this key then add a new cache value
        # with a count of 1 and interval starting at the current date and time.
        # Obviously, if nothing is cached then we can't have had too many
        # requests as this is the first one so we return False here.
        if current_count is None:
            cache.set(cache_key, (1, datetime.now()))
            return False
        else:
            # Calculate the time in seconds between the current date and time
            # and the interval start from the cached value.
            interval = (datetime.now() - current_count[1]).seconds

            # If we have exceeded the interval size then reset the interval
            # start time and reset the request count to 1 since we are on a
            # new interval now.
            if interval > limit_seconds:
                cache.set(cache_key, (1, datetime.now()))
                return False

            # Update the request count to account for this request
            new_count = current_count[0] + 1
            cache.set(cache_key, (new_count, current_count[1]))

            # Since we are still within the interval, just check if we have
            # exceeded the request limit or not and return the result of the
            # comparison.
            return new_count > limit_count
