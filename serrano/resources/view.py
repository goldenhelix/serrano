import functools
import logging
from datetime import datetime
from django.conf.urls import patterns, url
from django.core.urlresolvers import reverse
from django.views.decorators.cache import never_cache
from restlib2.http import codes
from preserialize.serialize import serialize
from avocado.models import DataView, DataConcept
from avocado.events import usage
from serrano.forms import ViewForm
from .base import ThrottledResource, extract_model_version
from .history import RevisionsResource, ObjectRevisionsResource, \
    ObjectRevisionResource
from . import templates

log = logging.getLogger(__name__)

def init_views(request, model_version_id):
    user = request.user 
    session = request.session._session_key
    user_id = None

    if str(user)=='AnonymousUser':
        view_query = "select avocado_dataview.id from avocado_dataview, auth_user where avocado_dataview.session_key='" + str(session) + "'"                     
    else:
        view_query = "select avocado_dataview.id from avocado_dataview, auth_user where avocado_dataview.user_id=auth_user.id and auth_user.username='" + str(user) + "'"
        try:
            user_id = DataView.objects.raw("select id from auth_user where username='" + str(user) + "';")[0].id
        except:
            user_id = 1;
            raise

    view_query = view_query + " and avocado_dataview.model_version_id=" + str(model_version_id) + ";"

    views = [v.id for v in  DataView.objects.raw(view_query)]
    concepts = [v.id for v in DataConcept.objects.filter(published=True, is_default=True, model_version_id=model_version_id)]
    if not views:
        json = {"ordering":[],"columns":[]}
        for concept_id in concepts:
            json["columns"].append(concept_id)
        if user_id:
            form = ViewForm(request, {'json': json, 'session': True, 'user_id':user_id, 'model_version_id':model_version_id})
        else:
            form = ViewForm(request, {'json': json, 'session': True, 'session_key':session, 'model_version_id':model_version_id})

        if form.is_valid():
            form.save()

def view_posthook(instance, data, request):
    uri = request.build_absolute_uri
    data['_links'] = {
        'self': {
            'href': uri(reverse('serrano:views:single', args=[instance.pk])),
        }
    }
    return data


class ViewBase(ThrottledResource):
    cache_max_age = 0
    private_cache = True

    model = DataView
    template = templates.View

    def prepare(self, request, instance, template=None):
        if template is None:
            template = self.template
        posthook = functools.partial(view_posthook, request=request)
        return serialize(instance, posthook=posthook, **template)

    def get_queryset(self, request, **kwargs):
        "Constructs a QuerySet for this user or session."

        if getattr(request, 'user', None) and request.user.is_authenticated():
            kwargs['user'] = request.user
        elif request.session.session_key:
            kwargs['session_key'] = request.session.session_key
        else:
            # The only case where kwargs is empty is for non-authenticated
            # cookieless agents.. e.g. bots, most non-browser clients since
            # no session exists yet for the agent.
            return self.model.objects.none()

        return self.model.objects.filter(**kwargs)

    def get_default(self, request):
        default = self.model.objects.get_default_template()

        if not default:
            log.warning('No default template for view objects')
            return

        form = ViewForm(request, {'json': default.json, 'session': True})

        if form.is_valid():
            instance = form.save()
            return instance

        log.error('Error creating default view', extra=dict(form.errors))


class ViewsResource(ViewBase):
    "Resource of views"
    def get(self, request):
        model_version = extract_model_version(request)

        init_views(request, model_version['id'])

        queryset = self.get_queryset(request)

        # Only create a default is a session exists
        if request.session.session_key:
            queryset = list(queryset)

            if not len(queryset):
                default = self.get_default(request)
                if default:
                    queryset.append(default)

        
        view = [v for v in self.prepare(request, queryset) if v['model_version_id']==model_version['id']]
        view = sorted(view, key=lambda r: r['modified'], reverse=True)

        return view

    def post(self, request):

        form = ViewForm(request, request.data)

        if form.is_valid():
            instance = form.save()
            usage.log('create', instance=instance, request=request)
            request.session.modified = True
            response = self.render(request, self.prepare(request, instance),
                                   status=codes.created)
        else:
            data = {
                'message': 'Cannot create view',
                'errors': dict(form.errors),
            }
            response = self.render(request, data,
                                   status=codes.unprocessable_entity)
        return response


class ViewResource(ViewBase):
    "Resource for accessing a single view"
    def get_object(self, request, pk=None, session=None, **kwargs):
        if not pk and not session:
            raise ValueError('A pk or session must used for the lookup')

        if not hasattr(request, 'instance'):
            queryset = self.get_queryset(request, **kwargs)

            try:
                if pk:
                    instance = queryset.get(pk=pk)
                else:
                    instance = queryset.get(session=True)
            except self.model.DoesNotExist:
                instance = None

            request.instance = instance

        return request.instance

    def is_not_found(self, request, response, **kwargs):
        return self.get_object(request, **kwargs) is None

    def get(self, request, **kwargs):
        instance = self.get_object(request, **kwargs)

        usage.log('read', instance=instance, request=request)
        self.model.objects.filter(pk=instance.pk).update(
            accessed=datetime.now())

        return self.prepare(request, instance)

    def put(self, request, **kwargs):
        instance = self.get_object(request, **kwargs)

        form = ViewForm(request, request.data, instance=instance)

        if form.is_valid():
            instance = form.save()
            usage.log('update', instance=instance, request=request)
            request.session.modified = True
            response = self.render(request, self.prepare(request, instance))
        else:
            data = {
                'message': 'Cannot update view',
                'errors': dict(form.errors),
            }
            response = self.render(request, data,
                                   status=codes.unprocessable_entity)
        return response

    def delete(self, request, **kwargs):
        instance = self.get_object(request, **kwargs)

        if instance.session:
            data = {
                'message': 'Cannot delete session view',
            }
            return self.render(request, data, status=codes.bad_request)

        instance.delete()
        usage.log('delete', instance=instance, request=request)
        request.session.modified = True


single_resource = never_cache(ViewResource())
active_resource = never_cache(ViewsResource())
revisions_resource = never_cache(RevisionsResource(
    object_model=DataView, object_model_template=templates.View,
    object_model_base_uri='serrano:views'))
revisions_for_object_resource = never_cache(ObjectRevisionsResource(
    object_model=DataView, object_model_template=templates.View,
    object_model_base_uri='serrano:views'))
revision_for_object_resource = never_cache(ObjectRevisionResource(
    object_model=DataView, object_model_template=templates.View,
    object_model_base_uri='serrano:views'))

# Resource endpoints
urlpatterns = patterns(
    '',
    url(r'^$', active_resource, name='active'),

    # Endpoints for specific views
    url(r'^(?P<pk>\d+)/$', single_resource, name='single'),
    url(r'^session/$', single_resource, {'session': True}, name='session'),

    # Revision related endpoints
    url(r'^revisions/$', revisions_resource, name='revisions'),
    url(r'^(?P<pk>\d+)/revisions/$', revisions_for_object_resource,
        name='revisions_for_object'),
    url(r'^(?P<object_pk>\d+)/revisions/(?P<revision_pk>\d+)/$',
        revision_for_object_resource, name='revision_for_object'),
)
