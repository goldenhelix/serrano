try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from django.conf.urls import patterns, url
from django.core.urlresolvers import reverse
from avocado.query import pipeline
from avocado.models import DataField, DataConcept;
from avocado.export import HTMLExporter
from restlib2.params import StrParam
from .base import BaseResource, extract_model_version, prune_view_columns, url_from_template, get_alias_map
from .view import init_views
from .pagination import PaginatorResource, PaginatorParametizer     
import json

def get_facet(facets, concept_id):
    for d in facets:
        if d['concept'] == concept_id:
            return d
    return {} 





def select_includes_schemas(select):
    for field in select:
        if 'record_' in field[0]:
            return True

    return False

class PreviewParametizer(PaginatorParametizer):
    processor = StrParam('default', choices=pipeline.query_processors)

class PreviewResource(BaseResource, PaginatorResource):
    """Resource for *previewing* data prior to exporting.

    Data is formatted using a JSON+HTML exporter which prefers HTML formatted
    or plain strings. Browser-based clients can consume the JSON and render
    the HTML for previewing.
    """
    parametizer = PreviewParametizer

    def get(self, request):
        params = self.get_params(request)

        # extract schema id and get list of concepts allowed by the schema 
        model_version = extract_model_version(request)

        valid_concepts = [c.id for c in DataConcept.objects.filter(model_version_id=model_version['id'])]

        # extract map of allowed values and type for each concept
        field_query = ("select avocado_dataconceptfield.concept_id, avocado_datafield.id, avocado_datafield.allowed_values, avocado_datafield.type from " 
        "avocado_dataconceptfield inner join avocado_datafield on avocado_dataconceptfield.field_id=avocado_datafield.id;")
        type_map = {c.concept_id:c.type for c in DataConcept.objects.raw(field_query)}

        
        view = self.get_view(request)

        page = params.get('page')
        limit = params.get('limit')
        tree = model_version['model_name']

        # Get the request's view and context
        init_views(request, model_version['id'])
       
        context = self.get_context(request)

        # Initialize a query processor
        QueryProcessor = pipeline.query_processors[params['processor']]
        processor = QueryProcessor(context=context, view=view, tree=tree)

        # Build a queryset for pagination and other downstream use
        queryset = processor.get_queryset(request=request)

        if select_includes_schemas(queryset.query.select):
            if 'assessment' in model_version['model_type']:
                rec_table = 'record_1'
                queryset.query.select.append((rec_table, "created"))
            else:
                queryset.query.select.append(('sample_record_schema', "created"))

        queryset.query.alias_map = get_alias_map(model_version['model_name'], queryset.query.alias_map) 

        # an ordering must be specified to ensure that paginator queries don't produce duplicates
        if model_version['model_type']=='sample':
            # only samples do not have genomic information
            queryset.query.order_by = queryset.query.order_by + ['id']
        elif model_version['model_type']=='project':
            queryset.query.distinct = False
        elif model_version['model_type']=='assessment':
            queryset.query.order_by = queryset.query.order_by + ['chr', 'pos_start', 'pos_stop', 'ref_alts']

        # Get paginator and page
        queryset.query.select = [s for s in queryset.query.select if s[0] in queryset.query.alias_map]
        paginator = self.get_paginator(queryset, limit=limit)
        page = paginator.page(page)
        offset = max(0, page.start_index() - 1)
        view_node = view.parse()

        # Build up the header keys.
        # TODO: This is flawed since it assumes the output columns
        # of exporter will be one-to-one with the concepts. This should
        # be built during the first iteration of the read, but would also
        # depend on data to exist!
        header = []
        ordering = OrderedDict(view_node.ordering)

        for concept in view_node.get_concepts_for_select():
            obj = {
                'id': concept.id,
                'name': concept.name
            }

            if concept.id in ordering:
                obj['direction'] = ordering[concept.id]

            header.append(obj)
        
        # find header indices that are allowed by the schema
        valid_indices = [i for i,h in enumerate(header) if h['id'] in valid_concepts]  
        header = [h for h in header if h['id'] in valid_concepts]

        # get the related datafields
        for h in header:
            field_query = 'select * from avocado_datafield where id = ANY(select field_id from avocado_dataconceptfield where concept_id=' + str(h['id']) + ');'
            related_field = DataField.objects.raw(field_query)[0]
            if related_field.keywords:
                keywords = json.loads(related_field.keywords)
                h['url_temp'] = keywords['urlTemplate']
        
        # Prepare an HTMLExporter
        exporter = processor.get_exporter(HTMLExporter)
        pk_name = queryset.model._meta.pk.name

        objects = []

        # 0 limit means all for pagination, however the read method requires
        # an explicit limit of None
        limit = limit or None

        # This is an optimization when concepts are selected for ordering
        # only. There is not guarantee to how many rows are required to get
        # the desired `limit` of rows, so the query is unbounded. If all
        # ordering facets are visible, the limit and offset can be pushed
        # down to the query.
        order_only = lambda f: not f.get('visible', True)

        # queryset spits out values of type Variant1
        queryset.query.distinct = False

        # add variant id to query if this is a variant results page
        variant_table = model_version['model_name']
        if (variant_table, '_id') not in queryset.query.select and 'project' in model_version['record_type']:        
            queryset.query.select.append((variant_table, '_id'))

        if filter(order_only, view_node.facets):
            iterable = processor.get_iterable(request=request,
                                              queryset=queryset)

            exported = exporter.read(iterable,
                                     request=request,
                                     offset=offset,
                                     limit=limit)
        else:
            iterable = processor.get_iterable(request=request,
                                              queryset=queryset,
                                              limit=limit,
                                              offset=offset)

            exported = exporter.read(iterable, request=request)
        
        row_count = 0
        header = [{'id':'_id', 'name':'_id'}] + header
        for row in exported:
            row_count += 1
            pk = None
            values = ['_id:' + str(row[0].get('_id', row[0].get('id', '')))]
            for i, output in enumerate(row):    
                # only show concepts that are valid according to the current schema
                if i-1 in valid_indices:
                    if i == 0:
                        pk = output[pk_name]
                    else:
                        values.extend(output.values())
            
                 
            for i in range(0, len(values)):
                urls = url_from_template(values[i], header[i].get('url_temp', None))
                values[i] = values[i].replace('"', '').replace("'", '').replace('[', '').replace(']', '')
                values[i] = values[i].replace('None', '?')
                if header[i]['name']=='Genomic Coordinate':                    
                    components = values[i].split(' ')
                    components[1] = str(int(components[1]) + 1)
                    values[i] = ' '.join(components)
                    values[i] = values[i].replace(' ', '_')
                else:
                    if type_map.get(header[i]['id'], 'String').startswith('Float'):
                        try:
                            values[i] = str(float(values[i]))
                        except ValueError:
                            values[i] = str(values[i])
                    if type_map.get(header[i]['id'], 'String').startswith('Integer'):
                        try:
                            values[i] = int(float(values[i]))
                        except ValueError:
                            values[i] = str(values[i])
                
                if urls:
                    links = []
                    for value, url in urls.iteritems():
                        links.append('<a target="_blank" href="' + url + '">' + value + '</a>')
                    values[i] = ', '.join(links)
                
                if values[i]==0:
                    values[i] = '0'

                elif not values[i] or values[i]=='null' or values[i]=='<em>n/a</em>' or all(v is None for v in values[i]): 
                    values[i] = '?'

            objects.append({
                'pk': pk,
                'values': values,
            })

        # Various model options
        opts = queryset.model._meta

        model_name = opts.verbose_name.format()
        model_name_plural = opts.verbose_name_plural.format()

        resp = self.get_page_response(request, paginator, page)

        path = reverse('serrano:data:preview')
        links = self.get_page_links(request, path, page, extra=params)

        resp.update({
            'keys': header,
            'objects': objects,
            'object_name': model_name,
            'object_name_plural': model_name_plural,
            'object_count': paginator.count,
            '_links': links,
        })

        return resp

    # POST mimics GET to support sending large request bodies for on-the-fly
    # context and view data.
    post = get


preview_resource = PreviewResource()

# Resource endpoints
urlpatterns = patterns('', url(r'^$', preview_resource, name='preview'), )
