import functools
import logging
from datetime import datetime
from django.conf.urls import patterns, url
from django.core.urlresolvers import reverse
from django.db import connection
from django.views.decorators.cache import never_cache
from restlib2.http import codes
from restlib2.params import Parametizer, StrParam
from preserialize.serialize import serialize
from modeltree.tree import trees
from avocado.events import usage
from avocado.models import DataContext, DataConcept, DataField
from avocado.query import pipeline
from ceviche.models import ModelVersion
from serrano.forms import ContextForm
from .base import ThrottledResource, extract_model_version
from .history import RevisionsResource, ObjectRevisionsResource, \
    ObjectRevisionResource
from . import templates
import copy
import hashlib
import json

log = logging.getLogger(__name__)

def get_all_samples(model_version_id):
    cursor = connection.cursor()
    model_version = ModelVersion.objects.get(id=model_version_id)
    entity_table = model_version.model_name + '_entity'
    query = ('SELECT ' + entity_table + '."samples" '
             'FROM ' + entity_table + ', sample_record_schema '
             'WHERE ' + entity_table + '.samples=sample_record_schema.id;');
    cursor.execute(query)
    samples = [r[0] for r in cursor.fetchall()]
    return samples

# pulls the sample info from the child if it exists
# then constructs a composite query with both the sample and query field 
def pull_samples(child, model_version_id, context_resource, request, processor, tree):
    try:
        sample_field = DataField.objects.get(model_version_id=model_version_id, field_name='samples', type='Sample')
    except:
        sample_field = None

    concept = child['concept']
    if type(child['value'])==list:
        try:
            sample_json = json.loads(child['value'][0])
        except (ValueError, TypeError) as e:
            return child

        if type(sample_json)==dict and 'samples' in sample_json:
            sample = sample_json['samples']
            if '_all_' in sample:
                sample = get_all_samples(model_version_id)
            child['value'] = child['value'][1]
            new_child = child
            if(sample):
                if type(child.get('operator')) is list and 'composite' not in child:
                    composite_id, language = build_composite_contexts(context_resource, request, child, processor, tree)
                    child = { 'composite': composite_id, 'field':child['field'], 'concept':child['concept'], 
                                  'language':language, 'operator':child['operator'], 'value':child['value']}

                sample_child  = {'concept':concept, 'language':'Sample', 'required':False, 'value':sample, 'field':sample_field.id, 'operator':'in'}
                and_id = save_composite_context(request, [sample_child, child], 'and', processor, tree)[0]

                if sample_json['cohort']=='custom cohort':
                    if len(sample) <= 4:
                        language = child['language'] + ' for ' + ','.join(sample)
                    else:
                        other_count = str(len(sample) - 3)
                        language = child['language'] + ' for ' + ','.join(sample[:3]) + ', and ' + other_count + ' others'
                else: 
                    language = child['language'] + ' for cohort ' + sample_json['cohort']    

                new_child = { 'composite': and_id, 'field':child['field'], 'concept':child['concept'], 
                                         'language':language, 'samples': sample, 'cohort':sample_json['cohort'], 'operator':child['operator'], 'value':child['value']}

            return new_child
    return child

def remove_quotes(value):
    if type(value)==list:
        for i, string in enumerate(value):
            if type(value[i])==str:
                value[i] = string.replace('"', '')
    elif type(value)==str:
        value = value.replace('"', '')

    return value

def build_composite_contexts(context, req, child, processor, tree):
    children = []
    logic = 'or'

    for i, op in enumerate(child['operator']):
        newchild = child.copy()
        newchild['operator'] = op
        newchild['value'] = child['value'][i]

        field_id = newchild.get('field', '')
        if field_id:
            data_type = DataField.objects.get(id=field_id).type
            if data_type=='Boolean' and type(newchild['value'])==list:
                newchild['value'].remove('Missing')

        # special case assumes that you will never combine NOT NULL with OR
        # Ex: (Quality=25 or Quality IS NOT NULL)  <- will never do this
        if newchild['operator']=='isnull' and newchild['value'] is False:
            logic = 'and'

        if not newchild['value']==[]:
            children.append(newchild)

    return save_composite_context(req, children, logic, processor, tree)

def save_composite_context(req, children, logic, processor, tree):
    if req:
        request = copy.copy(req)
        request.data['json']['type'] = logic
        request.data['json']['children'] = children
        form = ContextForm(request, request.data, processor=processor, tree=tree, json=request.data['json'], keywords='composite')
        
        instance = form.save(update_count=False)
        language = instance.json['children'][0]['language']
        for child in instance.json['children'][1:]:
            if 'Missing' not in language or not child['language']:
                language += ' ' + logic + ' ' + child['language']
         
        return instance.id, language
    else:
        return None, None

def get_chromosomes_between(chr1, chr2):
    # TODO: This hard-codes it to the human genome. We have
    # project-level info about what genome is being used, may need to
    # use it in the future here. No immediate action.
    chromosomes = [str(i) for i in range(1, 23)] + ['X', 'Y', 'M', 'MT']
    start = chromosomes.index(chr1)
    stop  = chromosomes.index(chr2)
    return chromosomes[start+1:stop] 

def build_gene_list_contexts(context, req, child, processor, tree, model_version_id):
    field = DataField.objects.get(id=child['field'])
    
    concept_query = ("select avocado_dataconceptfield.id, avocado_dataconcept.id as cid from avocado_dataconcept, avocado_dataconceptfield where "        
                     "avocado_dataconcept.id=avocado_dataconceptfield.concept_id and avocado_dataconceptfield.field_id=" + str(field.id) + ";")

    concept = [c.cid for c in DataConcept.objects.raw(concept_query)][0]

    language = 'Gene name matches ' + child['value'][1] + '. '
    values = child['value'][0].replace(' ', ',').replace(';', ',').replace('\n', ',').split(',')
    values = [v.strip() for v in values if v.strip()]
    shown_values = values
    if len(values)>4:
        shown_values = values[0:4] + ['...']

    if child['value'][1]=='custom gene list':
        language = 'Gene name matches ' + ', '.join(shown_values)
    else:
        language += 'Includes: ' + ', '.join(shown_values)

    query = {'concept':concept, 'language':language, 'required':False, 'value':values, 'field':field.id, 'operator':'in'}
    return save_composite_context(req, [query], 'and', processor, tree)[0], language 

def build_genomic_query(coordinates, model_version_id, tree, request=None, processor=None):
    field_query = ("select * from avocado_datafield where model_version_id='" + str(model_version_id) + "' and name in ('Pos Start', 'Pos Stop', 'Chromosome', 'Start', 'Stop', 'Chr');")
    fields = {f.name:f.id for f in DataField.objects.raw(field_query)}
    language = 'Coordinate overlaps with region ' + coordinates
    coordinates = coordinates.replace('Chr', '').replace('chr', '').replace(',', '')
    
    if 'Pos Start' in fields:
        chr_field = {'id':fields['Chromosome'], 'name':'Chromosome', 'symbol':'chr'}
        start_field = {'id':fields['Pos Start'], 'name':'Pos Start', 'symbol':'start'}
        stop_field  = {'id':fields['Pos Stop'], 'name':'Pos Stop', 'symbol':'stop'}
    else:
        if 'Chr' in fields:
            chr_field   = {'id':fields['Chr'], 'name':'Chr', 'symbol':'chr'}
        else:
            chr_field   = {'id':fields['Chromosome'], 'name':'Chr', 'symbol':'chr'}
        start_field = {'id':fields['Start'], 'name':'Start', 'symbol':'pos_start'}
        stop_field  = {'id':fields['Stop'], 'name':'Stop', 'symbol':'pos_stop'}

    concept_query = ("select avocado_dataconceptfield.id, avocado_dataconcept.id as cid from avocado_dataconcept, avocado_dataconceptfield where "        
                     "avocado_dataconcept.id=avocado_dataconceptfield.concept_id and avocado_dataconceptfield.field_id=" + str(stop_field['id']) + ";")

    concept = [c.cid for c in DataConcept.objects.raw(concept_query)][0]

    if ':' not in coordinates:
        if '-' in coordinates:
            contexts = []
            chr1 = coordinates.split('-')[0].strip()
            chr2 = coordinates.split('-')[1].strip()
            # construct chromosome position context for chromosomes between chr1 and chr2
            chromosomes = [chr1] + get_chromosomes_between(chr1, chr2) + [chr2]
            clauses = []
            for chromosome in chromosomes:
                clauses.append("chr = '" + chromosome + "'")
                chr_child = {'concept':concept, 'language':'Chromosome equals', 'required':False, 'value':chromosome, 'field':chr_field['id'], 'operator':'exact'}
                contexts.append(chr_child)

            context_query = save_composite_context(request, contexts, 'or', processor, tree)[0]
            sql_query = '(' + ' OR '.join(clauses) + ')'
        else:  
            chromosome = coordinates
            chr_query = {'concept':concept, 'language':'Chromosome equals', 'required':False, 'value':chromosome, 'field':chr_field['id'], 'operator':'exact'}
            context_query = save_composite_context(request, [chr_query], 'and', processor, tree)[0]
            sql_query = "chr = '" + chromosome + "'"
    elif coordinates.count(':')==1:
        chromosome, pos = coordinates.split(':')
        pos = [int(value) for value in pos.split('-')]
        start = pos[0]-1
        stop = None
        if len(pos)>1:
            stop = pos[1]

        # build CHR child
        chr_child = {'concept':concept, 'language':'Chromosome equals', 'required':False, 'value':chromosome, 'field':chr_field['id'], 'operator':'exact'}
        chr_query = "chr = '" + chromosome + "'"

        if stop:
            # if stop was specified then do a range query
            start_child = {'concept':concept, 'language':'Start in range', 'required':False, 'enabled':True, 
                           'value':stop, 'field':start_field['id'], 'operator':'lt'}
            stop_child  = {'concept':concept, 'language':'Stop in range', 'required':False, 'enabled':True, 
                           'value':start, 'field':stop_field['id'], 'operator':'gt'}
            start_query = start_field['symbol'] + ' BETWEEN ' + str(start)  + ' and ' + str(stop)
            stop_query  = stop_field['symbol']  + ' BETWEEN ' + str(start)  + ' and ' + str(stop)

            # build AND composite for chr and OR
            and_id = save_composite_context(request, [start_child, stop_child], 'and', processor, tree)[0]
            and_child  = {'concept':concept, 'language':'Start is in range or Stop is in range', 'composite':and_id}
            or_query  = '(' + start_query + ' OR ' + stop_query + ')'
            sql_query = '(' + or_query + ' AND ' + chr_query + ')'
            
            # (segment2Start == segment2Stop) && ((segment2Start == segment1Start) || (segment2Start == segment1Stop)) 
            context_query = save_composite_context(request, [chr_child, and_child], 'and', processor, tree)[0]
        else:
            # if stop was not specified then do an exact query
            start_child = {'concept':concept, 'language':'Start in range', 'required':False, 'enabled':True, 
                       'value':start, 'field':start_field['id'], 'operator':'exact'}
            start_query = start_field['symbol'] + ' = ' + str(start)
            sql_query = '(' + start_query + ' AND ' + chr_query + ')'
            context_query = save_composite_context(request, [chr_child, start_child], 'and', processor, tree)[0]
    elif coordinates.count(':')==2:
        chr1 = coordinates.split(':')[0].strip()
        chr2 = coordinates.split('-')[1].split(':')[0].strip()
        start = str(int(coordinates.split('-')[0].split(':')[1])-1)
        stop = coordinates.split('-')[1].split(':')[1]

        clauses = []
        contexts = []
        # construct sql for condition ((variant.stop>query.start or variant.start>query.start) and variant.chr=query.chr1)
        first_start_query = start_field['symbol'] + ' >= ' + str(start)
        first_stop_query  = stop_field['symbol']  + ' >= ' + str(start)
        first_or_query = '(' + first_start_query + ' OR ' + first_stop_query + ')'
        first_chr_query = "chr = '" + chr1 + "'"
        first_and_query = '(' + first_or_query + ' AND ' + first_chr_query + ')'
        clauses.append(first_and_query)

        # construct context for condition ((variant.stop>query.start or variant.start>query.start) and variant.chr=query.chr1)
        first_start_child = {'concept':concept, 'language':'Start in range', 'required':False, 'enabled':True, 
                             'value':start, 'field':start_field['id'], 'operator':'gt'}
        first_stop_child  = {'concept':concept, 'language':'Stop in range', 'required':False, 'enabled':True, 
                             'value':start, 'field':stop_field['id'], 'operator':'gt'}
        first_or_id = save_composite_context(request, [first_start_child, first_stop_child], 'or', processor, tree)[0]
        first_or_child  = {'concept':concept, 'language':'Start is in range or Stop is in range', 'composite':first_or_id}
        first_chr_child = {'concept':concept, 'language':'Chromosome equals', 'required':False, 'value':chr1, 'field':chr_field['id'], 'operator':'exact'}
        first_and_id = save_composite_context(request, [first_chr_child, first_or_child], 'and', processor, tree)[0]
        first_and_child = {'concept':concept, 'language':'Start or Stop is in first chromosome range', 'composite':first_and_id}
        contexts.append(first_and_child)

        # construct chromosome position context for chromosomes between chr1 and chr2        
        chromosomes = get_chromosomes_between(chr1, chr2)
        for chromosome in chromosomes:
            clauses.append("chr = '" + chromosome + "'")
            chr_child_json = {'concept':concept, 'language':'Chromosome equals', 'required':False, 'value':chromosome, 'field':chr_field['id'], 'operator':'exact'}
            contexts.append(chr_child_json)
            
        # construct sql for condition ((variant.stop<query.stop or variant.start>query.stop) and variant.chr=query.chr2)
        last_start_query = start_field['symbol'] + '<= ' + str(stop)
        last_stop_query  = stop_field['symbol']  + '<= ' + str(stop)
        last_or_query    = '(' + last_start_query + ' OR ' + last_stop_query + ')'
        last_chr_query = "chr = '" + chr2 + "'"
        last_and_query = '(' + last_or_query + ' AND ' + last_chr_query + ')'
        clauses.append(last_and_query)
        sql_query = '(' + ' OR '.join(clauses) + ')'

        # construct json for condition ((variant.stop<query.stop or variant.start>query.stop) and variant.chr=query.chr2)
        last_start_child = {'concept':concept, 'language':'Start in range', 'required':False, 'enabled':True, 
                            'value':stop, 'field':start_field['id'], 'operator':'lt'}
        last_stop_child  = {'concept':concept, 'language':'Stop in range', 'required':False, 'enabled':True, 
                             'value':stop, 'field':stop_field['id'], 'operator':'lt'}
        last_or_id = save_composite_context(request, [last_start_child, last_stop_child], 'or', processor, tree)[0]
        last_or_child   = {'concept':concept, 'language':'Start is in range or Stop is in range', 'composite':last_or_id}
        last_chr_child  = {'concept':concept, 'language':'Chromosome equals', 'required':False, 'value':chr2, 'field':chr_field['id'], 'operator':'exact'}
        last_and_id = save_composite_context(request, [last_chr_child, last_or_child], 'and', processor, tree)[0]
        last_and_child = {'concept':concept, 'language':'Start or Stop is in last chromosome range', 'composite':last_and_id}
        contexts.append(last_and_child)
        context_query = save_composite_context(request, contexts, 'or', processor, tree)[0]

    return context_query, sql_query, language

def build_genomic_contexts(context, req, child, processor, tree, model_version_id):
    context_query, sql_query, language = build_genomic_query(child['value'], model_version_id, tree, request=req, processor=processor)
    return context_query, language
    
def gen_id(s):
    #use hash to generate id
    md5 =  hashlib.md5()
    md5.update(s)

    # chop of digest so that we don't exceed postgres int limits
    # chance of collision is still less than 1 in 10^19
    d = md5.hexdigest()[:6]
    return int(d, 16)

def context_posthook(instance, data, request, tree):
    uri = request.build_absolute_uri

    opts = tree.root_model._meta
    data['object_name'] = opts.verbose_name.format()
    data['object_name_plural'] = opts.verbose_name_plural.format()

    data['_links'] = {
        'self': {
            'href': uri(
                reverse('serrano:contexts:single', args=[instance.pk])),
        },
        'stats': {
            'href': uri(reverse('serrano:contexts:stats', args=[instance.pk])),
        }
    }
    return data


def update_children(context_resource, model_version_id, model_type, request, processor, tree):
    if 'json' in request.data and request.data['json']:
        newchildren = []
        for child in request.data['json']['children']:
            if model_type=='project':
                child['value'] = remove_quotes(child['value'])  
    
                # handle null values
                if type(child['value'])==list:
                    for i, v in enumerate(child['value']):
                        if v is None:
                            child['value'][i] = 'None'
                elif child['value'] is None:
                    child['value'] = 'None'

            composite_id = None
            child = pull_samples(child, model_version_id, context_resource, request, processor, tree)
            if child.get('operator')=='genomic-coordinate':
                composite_id, language = build_genomic_contexts(context_resource, request, child, processor, tree, model_version_id)
            elif child.get('operator')=='match-list':
                composite_id, language = build_gene_list_contexts(context_resource, request, child, processor, tree, model_version_id)
            elif type(child.get('operator')) is list and 'composite' not in child:
                composite_id, language = build_composite_contexts(context_resource, request, child, processor, tree)

            if composite_id: 
                child = { 'composite': composite_id, 'field':child['field'], 'concept':child['concept'], 'enabled':child.get('enabled', True),
                                     'language':language, 'operator':child['operator'], 'value':child['value']}
                
                newchildren.append(child)
            else:

                newchildren.append(child)
        
        request.data['json']['children'] = newchildren
        request.data['json']['type'] = 'and' 

    return request


class ContextParametizer(Parametizer):
    processor = StrParam('default', choices=pipeline.query_processors)


class ContextBase(ThrottledResource):
    cache_max_age = 0
    private_cache = True

    model = DataContext
    template = templates.Context

    parametizer = ContextParametizer

    def prepare(self, request, instance, tree, template=None):
        if template is None:
            template = self.template

        tree = trees[tree]
        posthook = functools.partial(context_posthook, request=request, tree=tree)
        serial = serialize(instance, posthook=posthook, **template)

        return serial

    def get_queryset(self, request, **kwargs):
        "Constructs a QuerySet for this user or session."

        model_version = extract_model_version(request)
        kwargs['model_version_id'] = model_version['id']

        if getattr(request, 'user', None) and request.user.is_authenticated():
            kwargs['user'] = request.user
        elif request.session.session_key:
            kwargs['session_key'] = request.session.session_key
        else:
            # The only case where kwargs is empty is for non-authenticated
            # cookieless agents.. e.g. bots, most non-browser clients since
            # no session exists yet for the agent.
            return self.model.objects.none()
        
        queryset = self.model.objects.filter(**kwargs)
        return queryset

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

    def get_default(self, request):
        default = self.model.objects.get_default_template()

        if not default:
            log.warning('No default template for context objects')
            return

        form = ContextForm(request, {'json': default.json, 'session': True})

        instance = form.save()
        return instance

        log.error('Error creating default context', extra=dict(form.errors))


class ContextsResource(ContextBase):
    "Resource of contexts"
    def get(self, request):
        queryset = self.get_queryset(request)

        
        # Only create a default if a session exists
        if request.session.session_key:
            queryset = list(queryset)

            if not len(queryset):
                default = self.get_default(request)
                if default:
                    queryset.append(default)

        model_version = extract_model_version(request)
        schema_tree = model_version['model_name']

        prep = [q for q in self.prepare(request, queryset, tree=schema_tree) if 'composite' not in q['keywords'] ]

        return prep

    def post(self, request):
        params = self.get_params(request)

        model_version = extract_model_version(request)

        tree = model_version['model_name']

        processor = params['processor']

        request = update_children(self, model_version['id'], model_version['model_type'], request, processor, tree)
        
        form = ContextForm(request, request.data, processor=processor, tree=tree, json=request.data['json'])
        instance = form.save()
        usage.log('create', instance=instance, request=request)

        request.session.modified = True

        data = self.prepare(request, instance, tree=tree)

        render = self.render(request, data, status=codes.created)

        return render


class ContextResource(ContextBase):
    "Resource for accessing a single context"
    def is_not_found(self, request, response, **kwargs):
        return self.get_object(request, **kwargs) is None

    def get(self, request, **kwargs):
        instance = self.get_object(request, **kwargs)
        usage.log('read', instance=instance, request=request)

        # Fast single field update..
        # TODO Django 1.5+ supports this on instance save methods.
        self.model.objects.filter(pk=instance.pk).update(
            accessed=datetime.now())

        model_version = extract_model_version(request)

        prep = self.prepare(request, instance, tree=model_version['model_name'])

        return prep

    

    def put(self, request, **kwargs):
        params = self.get_params(request)
        model_version = extract_model_version(request)

        tree = model_version['model_name']
        processor = params['processor']

        request = update_children(self, model_version['id'], model_version['model_type'], request, processor, tree)   
        instance = self.get_object(request, **kwargs)

        form = ContextForm(request, request.data, instance=instance, processor=processor, tree=tree, json=request.data['json'])
        instance = form.save()

        usage.log('update', instance=instance, request=request)

        request.session.modified = True
        data = self.prepare(request, instance, tree=tree)
        render = self.render(request, data)
        return render

    def delete(self, request, **kwargs):
        instance = self.get_object(request, **kwargs)

        # Cannot delete the current session
        if instance.session:
            data = {
                'message': 'Cannot delete session context',
            }
            return self.render(request, data, status=codes.bad_request)

        instance.delete()
        usage.log('delete', instance=instance, request=request)
        request.session.modified = True


class ContextStatsResource(ContextBase):
    def is_not_found(self, request, response, **kwargs):
        return self.get_object(request, **kwargs) is None

    def get(self, request, **kwargs):
        instance = self.get_object(request, **kwargs)

        count = instance.apply().distinct().count()

        return {
            'count': count
        }


single_resource = never_cache(ContextResource())
stats_resource = never_cache(ContextStatsResource())
active_resource = never_cache(ContextsResource())
revisions_resource = never_cache(RevisionsResource(
    object_model=DataContext, object_model_template=templates.Context,
    object_model_base_uri='serrano:contexts'))
revisions_for_object_resource = never_cache(ObjectRevisionsResource(
    object_model=DataContext, object_model_template=templates.Context,
    object_model_base_uri='serrano:contexts'))
revision_for_object_resource = never_cache(ObjectRevisionResource(
    object_model=DataContext, object_model_template=templates.Context,
    object_model_base_uri='serrano:contexts'))

# Resource endpoints
urlpatterns = patterns(
    '',
    url(r'^$', active_resource, name='active'),

    # Endpoints for specific contexts
    url(r'^(?P<pk>\d+)/$', single_resource, name='single'),
    url(r'^session/$', single_resource, {'session': True}, name='session'),

    # Stats for a single context
    url(r'^(?P<pk>\d+)/stats/$', stats_resource,  name='stats'),
    url(r'^session/stats/$', stats_resource, {'session': True}, name='stats'),

    # Revision related endpoints
    url(r'^revisions/$', revisions_resource, name='revisions'),
    url(r'^(?P<pk>\d+)/revisions/$', revisions_for_object_resource,
        name='revisions_for_object'),
    url(r'^(?P<object_pk>\d+)/revisions/(?P<revision_pk>\d+)/$',
        revision_for_object_resource, name='revision_for_object'),
)
