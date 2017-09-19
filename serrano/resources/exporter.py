from datetime import datetime
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.conf.urls import patterns, url
from django.core.urlresolvers import reverse
from django.shortcuts import render
from restlib2.params import Parametizer, IntParam, StrParam
from avocado.export import registry as exporters
from avocado.query import pipeline
from avocado.events import usage
from ..conf import settings
from . import API_VERSION
from .base import BaseResource, extract_model_version, prune_view_columns

# Single list of all registered exporters
EXPORT_TYPES = zip(*exporters.choices)[0]


class ExporterRootResource(BaseResource):
    def get(self, request):
        uri = request.build_absolute_uri

        resp = {
            'title': 'Serrano Exporter Endpoints',
            'version': API_VERSION,
            '_links': {
                'self': {
                    'href': uri(reverse('serrano:data:exporter')),
                },
            }
        }

        for export_type in EXPORT_TYPES:
            resp['_links'][export_type] = {
                'href': uri(reverse('serrano:data:exporter',
                                    kwargs={'export_type': export_type})),
                'title': exporters.get(export_type).short_name,
                'description': exporters.get(export_type).long_name,
            }
        return resp


class ExporterParametizer(Parametizer):
    limit = IntParam(20)
    processor = StrParam('default', choices=pipeline.query_processors)

class ExporterResource(BaseResource):
    cache_max_age = 0

    private_cache = True

    parametizer = ExporterParametizer

    def _export(self, request, export_type, view, context, **kwargs):
        # Handle an explicit export type to a file
        resp = HttpResponse()
        params = self.get_params(request)
        model_version = extract_model_version(request)

        limit = params.get('limit')
        tree = model_version['model_name']
        page = kwargs.get('page')
        stop_page = kwargs.get('stop_page')
        offset = None

        # Restrict export to a particular page or page range
        if page:
            page = int(page)

            # Pages are 1-based
            if page < 1:
                raise Http404

            # Change to 0-base for calculating offset
            offset = limit * (page - 1)

            if stop_page:
                stop_page = int(stop_page)

                # Cannot have a lower index than page
                if stop_page < page:
                    raise Http404

                # 4...5 means 4 and 5, not everything up to 5 like with
                # list slices, so 4...4 is equivalent to just 4
                if stop_page > page:
                    limit = limit * ((stop_page-page)+1)

        else:
            # When no page or range is specified, the limit does not apply.
            limit = None

        QueryProcessor = pipeline.query_processors[params['processor']]
        view =  prune_view_columns(view, model_version['id'])
        processor = QueryProcessor(context=context,
                                   view=view,
                                   tree=tree,
                                   include_pk=False)

        queryset = processor.get_queryset(request=request)


        queryset.query.distinct = True
        if model_version['model_type']=='project':
            queryset.query.order_by = []
            queryset.query.distinct = False
        elif model_version['record_type'] in ['region', 'cnv']:
            queryset.query.order_by = queryset.query.order_by + ['chr', 'pos_start', 'pos_stop']
            for key in model_version["keys"]:
                queryset.query.order_by.append(key["symbol"])
        elif not model_version['record_type']=='sample':
            queryset.query.order_by = queryset.query.order_by + ['chr', 'pos_start', 'pos_stop', 'ref_alts']

        exporter = processor.get_exporter(exporters[export_type])

        view_node = view.parse()

        # This is an optimization when concepts are selected for ordering
        # only. There is not guarantee to how many rows are required to get
        # the desired `limit` of rows, so the query is unbounded. If all
        # ordering facets are visible, the limit and offset can be pushed
        # down to the query.
        order_only = lambda f: not f.get('visible', True)

        generator = getattr(exporter, "generator", None)
        if filter(order_only, view_node.facets):
            iterable = processor.get_iterable(request=request,
                                              queryset=queryset)

            # Write the data to the response
            exporter.write(iterable,
                           resp,
                           request=request,
                           offset=offset,
                           limit=limit,
                           model_version_id=model_version['id'], model_type=model_version['model_type'])
        else:
            iterable = processor.get_iterable(request=request,
                                              queryset=queryset,
                                              limit=limit,
                                              offset=offset)

            if callable(generator): 
                resp = StreamingHttpResponse(exporter.generator(iterable,
                               request=request,
                               model_version_id=model_version['id'], model_type=model_version['model_type']))
            else:
                exporter.write(iterable,
                               resp,
                               request=request,
                               model_version_id=model_version['id'], model_type=model_version['model_type'])


        filename = model_version['series_name'] + ' - ' + datetime.now().strftime('%Y-%m-%d') + '.' + exporter.file_extension

        cookie_name = settings.EXPORT_COOKIE_NAME_TEMPLATE.format(export_type)
        resp.set_cookie(cookie_name, settings.EXPORT_COOKIE_DATA)

        resp['Content-Disposition'] = 'attachment; filename="{0}"'.format(filename)                                 
        resp['Content-Type'] = exporter.content_type

        usage.log('export', request=request, data={
            'type': export_type,
            'partial': page is not None,
        })
        
        return resp

    # Resource is dependent on the available export types
    def is_not_found(self, request, response, export_type, **kwargs):
        return export_type not in EXPORT_TYPES

    def get(self, request, export_type, **kwargs):
        model_version = extract_model_version(request)
        if model_version:
            view = self.get_view(request)
            context = self.get_context(request)
            view = prune_view_columns(view, model_version['id'])
            
            resp = self._export(request, export_type, view, context, **kwargs)
            return resp
        else:
            message = 'This download has expired.'
            return render(request, 'message.html', {'title': 'Download Expired', 'message':message, 'restricted':True})

    post = get


exporter_resource = ExporterResource()
exporter_root_resource = ExporterRootResource()

# Resource endpoints
urlpatterns = patterns(
    '',
    url(r'^$', exporter_root_resource, name='exporter'),
    url(r'^(?P<export_type>\w+)/$', exporter_resource, name='exporter'),
    url(r'^(?P<export_type>\w+)/(?P<page>\d+)/$', exporter_resource,
        name='exporter'),
    url(r'^(?P<export_type>\w+)/(?P<page>\d+)\.\.\.(?P<stop_page>\d+)/$',
        exporter_resource, name='exporter'),
)
