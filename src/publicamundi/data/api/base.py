import logging

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import DBAPIError

import json
import geojson

import shapely.wkb
import shapely.wkt
import shapely.geometry.base

import numbers
import os
import string
import time

log = logging.getLogger(__name__)

# Available formats
QUERY_FORMAT_JSON = 'JSON'
QUERY_FORMAT_GEOJSON = 'GeoJSON'

# Supported formats
FORMAT_SUPPORT_QUERY = [QUERY_FORMAT_JSON , QUERY_FORMAT_GEOJSON]

CRS_SUPPORTED = ['EPSG:900913', 'EPSG:3857', 'EPSG:4326', 'EPSG:2100', 'EPSG:4258']
CRS_DEFAULT_DATABASE = 2100
CRS_DEFAULT_OUTPUT = 3857

OP_LIKE = 'LIKE'
OP_EQ = 'EQUAL'
OP_NOT_EQ = 'NOT_EQUAL'
OP_GT = 'GREATER'
OP_GET = 'GREATER_OR_EQUAL'
OP_LT = 'LESS'
OP_LET = 'LESS_OR_EQUAL'

OP_AREA = 'AREA'
OP_DISTANCE = 'DISTANCE'
OP_CONTAINS = 'CONTAINS'
OP_INTERSECTS = 'INTERSECTS'

COMPARE_OPERATORS = [OP_EQ, OP_NOT_EQ, OP_GT, OP_GET, OP_LT, OP_LET, OP_LIKE]
COMPARE_EXPRESSIONS = ['=', '<>', '>', '>=', '<', '<=', 'like']

SPATIAL_COMPARE_OPERATORS = [OP_EQ, OP_GT, OP_GET, OP_LT, OP_LET]
SPATIAL_OPERATORS = [OP_AREA, OP_DISTANCE, OP_CONTAINS, OP_INTERSECTS]

ALL_OPERATORS = [OP_EQ, OP_NOT_EQ, OP_GT, OP_GET, OP_LT, OP_LET, OP_LIKE, OP_AREA, OP_DISTANCE, OP_CONTAINS, OP_INTERSECTS]

MAX_RESULT_ROWS = 10000

CONFIG_SQL_CATALOG = 'sqlalchemy.catalog'
CONFIG_SQL_DATA = 'sqlalchemy.vectorstore'
CONFIG_SQL_TIMEOUT = 'timeout'

DEFAULT_SQL_TIMEOUT = 30000

# See http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
_PG_ERR_CODE = {
    'query_canceled': '57014',
    'undefined_object': '42704',
    'syntax_error': '42601',
    'permission_denied': '42501'
}
class DataException(Exception):
    def __init__(self, message, innerException=None):
        self.message = message
        self.innerException = innerException

    def __str__(self):
        return repr(self.message)

class QueryExecutor:

    def execute(self, config, query, metadata={}):
        try:
            engine_ckan = None
            connection_ckan = None

            engine_data = None
            connection_data = None

            output_format = QUERY_FORMAT_GEOJSON
            crs = CRS_DEFAULT_OUTPUT

            # Set CRS
            if 'crs' in query:
                if not query['crs'] in CRS_SUPPORTED:
                    raise DataException('CRS {crs} is not supported.'.format(format = query['crs']))

                crs = int(query['crs'].split(':')[1])

            # Set format
            if 'format' in query:
                if not query['format'] in FORMAT_SUPPORT_QUERY:
                    raise DataException('Output format {format} is not supported for query results.'.format(format = query['format']))

                output_format = query['format']

            # Get queue
            if not 'queue' in query:
                raise DataException('Parameter queue is required.')

            if not type(query['queue']) is list or len(query['queue']) == 0:
                raise DataException('Parameter queue should be a list with at least one item.')

            # Initialize database
            engine_ckan = create_engine(config[CONFIG_SQL_CATALOG], echo=False)
            engine_data = create_engine(config[CONFIG_SQL_DATA], echo=False)
            connection_ckan = engine_ckan.connect()
            connection_data = engine_data.connect()

            # Initialize execution context
            context = {
                'query' : None,
                'output_format' : output_format,
                'crs' : crs,
                'engine_ckan' : engine_ckan,
                'engine_data' : engine_data,
                'connection_ckan' : connection_ckan,
                'connection_data' : connection_data,
                'resources' : self.getResources(config, connection_ckan),
                'metadata' : metadata,
                'elapsed_time' : 0
            }

            # Execute queries
            query_result = []

            for q in query['queue']:
                context['query'] = q

                partial_result = self._execute_query(config, context)

                if output_format == QUERY_FORMAT_GEOJSON:
                    partial_result = {
                        'features': partial_result,
                        'type': 'FeatureCollection'
                    }

                query_result.append(partial_result)

            return {
                'data' : query_result,
                'crs' : crs,
                'metadata' : context['metadata'],
                'format' : output_format
            }
        except DataException as apiEx:
            raise
        except DBAPIError as dbEx:
            print dbEx.message
            log.error(dbEx)

            message = 'Unhandled exception has occured.'
            if dbEx.orig.pgcode == _PG_ERR_CODE['query_canceled']:
                message = 'Execution exceeded timeout.'

            raise DataException(message, dbEx)
        except Exception as ex:
            log.error(ex)

            raise DataException('Unhandled exception has occured.', ex)
        finally:
            if not connection_ckan is None:
                connection_ckan.close()
            if not connection_data is None:
                connection_data.close()

    def _execute_query(self, config, context):
        query = context['query']
        output_format = context['output_format']

        engine_ckan = context['engine_ckan']
        connection_ckan = context['connection_ckan']

        engine_data = context['engine_data']
        connection_data = context['connection_data']

        srid = context['crs']
        timeout = config[CONFIG_SQL_TIMEOUT] if CONFIG_SQL_TIMEOUT in config else DEFAULT_SQL_TIMEOUT
        offset = 0
        limit = MAX_RESULT_ROWS

        result = []

        count_geom_columns = 0;

        parsed_query = {
            'resources' : {},
            'fields': {},
            'filters' : [],
            'sort' : []
        }

        # Get limit
        if 'limit' in query:
            if not isinstance(query['limit'], numbers.Number):
                raise DataException('Parameter limit must be a number.')
            if query['limit'] < limit and query['limit'] > 0 :
                limit = query['limit']

        # Get offset
        if 'offset' in query:
            if not isinstance(query['offset'], numbers.Number):
                raise DataException('Parameter offset must be a number.')
            if query['offset'] >= 0:
                offset = query['offset']

        # Get resources
        if not 'resources' in query:
            raise DataException('No resource selected.')

        if not type(query['resources']) is list:
            raise DataException('Parameter resource should be a list with at least one item.')

        # Same as metadata but contains only the resources that are being accessed by the specific query
        query_metadata = {}
        # Used for managing resource name to alias mappings
        resource_mapping = {}

        for query_resource in query['resources']:
            db_resource = None

            resource_name = None
            resource_alias = None

            if type(query_resource) is dict:
                if 'name' in query_resource:
                    resource_name = query_resource['name']
                else:
                    raise DataException('Resource name is missing.')
                if 'alias' in query_resource:
                    resource_alias = query_resource['alias']
                else:
                    # If no alias is set, the name of the resources becomes an alias by default
                    resource_alias = resource_name
            elif isinstance(query_resource, basestring):
                resource_name = query_resource
                resource_alias = query_resource
            else:
                raise DataException('Resource parameter is malformed. Instance of string or dictionary is expected.')

            # Mappings for handling aliases
            resource_mapping[resource_name] = resource_name
            resource_mapping[resource_alias] = resource_name

            if resource_name in context['resources']:
                db_resource = context['resources'][resource_name]

                if not resource_name in context['metadata']:
                    # Update alias. Alias is reseted for every query execution
                    db_resource['alias'] = 't{index}'.format(index = (len(context['metadata'].keys()) + 1))

                    # Add fields
                    db_fields = self.describeResource(config, connection_data, resource_name)
                    db_resource['srid'] = db_fields['srid']
                    db_resource['geometry_column'] = db_fields['geometry_column']
                    db_resource['fields'] = db_fields['fields']

                    # Add resource to global metadata
                    context['metadata'][resource_name] = db_resource
                else:
                    db_resource = context['metadata'][resource_name]

                parsed_query['resources'][resource_name] = {
                    'table' : db_resource['table'],
                    'alias' : db_resource['alias']
                }

                # Add resource to local metadata
                query_metadata[resource_name] = db_resource
            else:
                raise DataException('Resource {resource} does not exist.'.format(
                    resource = resource_name
                ))

        # If no fields are selected, all fields are added to the response.
        # This may result in some fields names being ambiguous.
        addAllFields = False
        if not 'fields' in query:
            addAllFields = True
        elif not type(query['fields']) is list:
            raise DataException('Parameter fields should be a list.')
        elif len(query['fields']) == 0:
            addAllFields = True

        if addAllFields:
            query['fields'] = []
            for resource in query_metadata:
                for field in query_metadata[resource]['fields']:
                    query['fields'].append({
                        'resource' : resource,
                        'name' :  query_metadata[resource]['fields'][field]['name']
                    })

        # Get fields
        for i in range(0, len(query['fields'])):
            field_resource = None
            field_name = None
            field_alias = None

            if type(query['fields'][i]) is dict:
                if 'name' in query['fields'][i]:
                    field_name = query['fields'][i]['name']
                else:
                    raise DataException('Field name is missing.')
                if 'alias' in query['fields'][i]:
                    field_alias = query['fields'][i]['alias']
                else:
                    # If no alias is set, the name of the field becomes an alias by default
                    field_alias = field_name
                if 'resource' in query['fields'][i]:
                    field_resource = query['fields'][i]['resource']
            elif isinstance(query['fields'][i], basestring):
                field_name = query['fields'][i]
                field_alias = query['fields'][i]
            else:
                raise DataException('Field is malformed. Instance of string or dictionary is expected.')

            # Set resource if not set
            if field_resource is None:
                resources = self._get_resources_by_field_name(query_metadata, field_name)
                if len(resources) == 0:
                    raise DataException(u'Field {field} does not exist.'.format(
                        field = field_name
                    ))
                elif len(resources) == 1:
                    field_resource = resources[0]
                else:
                    raise DataException(u'Field {field} is ambiguous for resources {resources}.'.format(
                        field = field_name,
                        resources = u','.join(resources)
                    ))

            if not field_resource in resource_mapping or not resource_mapping[field_resource] in query_metadata:
                raise DataException(u'Resource {resource} for field {field} does not exist.'.format(
                    resource = field_resource,
                    field = field_name
                ))

            db_resource = query_metadata[resource_mapping[field_resource]]

            if field_name in db_resource['fields']:
                db_field = db_resource['fields'][field_name]

                if field_alias in parsed_query['fields']:
                   raise DataException(u'Field {field} in resource {resource} is ambiguous.'.format(
                        field = db_field['name'],
                        resource = field_resource
                    ))

                parsed_query['fields'][field_alias] = {
                    'fullname' : '{table}."{field}"'.format(
                        table = db_resource['alias'],
                        field = db_field['name']
                    ),
                    'name' : db_field['name'],
                    'alias' : field_alias,
                    'type' : db_field['type'],
                    'is_geom' : True if db_field['name'] == db_resource['geometry_column'] else False,
                    'srid' :  db_resource['srid'] if db_field['name'] == db_resource['geometry_column'] else None
                }
            else:
                raise DataException(u'Field {field} does not exist in resource {resource}.'.format(
                    field = field_name,
                    resource = field_resource
                ))

        # Check the number of geometry columns
        if output_format == QUERY_FORMAT_GEOJSON:
            count_geom_columns = reduce(lambda x, y: x+y, [1 if parsed_query['fields'][field]['is_geom'] else 0 for field in parsed_query['fields'].keys()])
            if count_geom_columns != 1:
                raise DataException(u'Format {format} requires exactly one geometry column'.format(
                    format = output_format
                ))

        # Get constraints
        if 'filters' in query and not type(query['filters']) is list:
            raise DataException(u'Parameter filters should be a list with at least one item.')

        if 'filters' in query and len(query['filters']) > 0:
            for f in query['filters']:
                parsed_query['filters'].append(self._create_filter(query_metadata, resource_mapping, f))

        # Get order by
        if 'sort' in query:
            if not type(query['sort']) is list:
                raise DataException('Parameter sort should be a list.')
            elif len(query['sort']) > 0:
                for i in range(0, len(query['sort'])):
                    # Get sort field properties
                    sort_resource = None
                    sort_name = None
                    sort_desc = False

                    if type(query['sort'][i]) is dict:
                        if 'name' in query['sort'][i]:
                            sort_name = query['sort'][i]['name']
                        else:
                            raise DataException('Sorting field name is missing.')
                        if 'resource' in query['sort'][i]:
                            sort_resource = query['sort'][i]['resource']
                        if 'desc' in query['sort'][i] and isinstance(query['sort'][i]['desc'], bool):
                            sort_desc = query['sort'][i]['desc']
                    elif isinstance(query['sort'][i], basestring):
                        sort_name = query['sort'][i]
                    else:
                        raise DataException('Sorting field is malformed. Instance of string or dictionary is expected.')

                    # Check if a field name or an alias is specified. In the latter case, set the database field name
                    if sort_name in parsed_query['fields']:
                        if parsed_query['fields'][sort_name]['name'] != sort_name:
                           sort_name = parsed_query['fields'][sort_name]['name']

                    # Set resource if missing
                    if sort_resource is None:
                        resources = self._get_resources_by_field_name(query_metadata, sort_name)

                        if len(resources) == 0:
                            raise DataException(u'Sorting field {field} does not exist.'.format(
                                field = sort_name
                            ))
                        elif len(resources) == 1:
                            sort_resource = resources[0]
                        else:
                            raise DataException(u'Sorting field {field} is ambiguous for resources {resources}.'.format(
                                field = sort_name,
                                resources = u','.join(resources)
                            ))

                    # Check if resource exists in metadata
                    if not sort_resource in resource_mapping or not resource_mapping[sort_resource] in query_metadata:
                        raise DataException(u'Resource {resource} for sorting field {field} does not exist.'.format(
                            resource = sort_resource,
                            field = sort_name
                        ))

                    parsed_query['sort'].append('{table}."{field}" {desc}'.format(
                        table = query_metadata[resource_mapping[sort_resource]]['alias'],
                        field = sort_name,
                        desc = 'desc' if sort_desc else ''
                    ))

        # Build SQL command
        fields = []
        tables = []
        wheres = []
        values = ()
        where_clause = ''
        orderby_clause = ''

        # Select clause fields
        for field in parsed_query['fields']:
            if parsed_query['fields'][field]['is_geom'] and parsed_query['fields'][field]['srid'] != srid:
                fields.append('ST_Transform({geom}, {srid}) as "{alias}"'.format(
                    geom = parsed_query['fields'][field]['fullname'],
                    srid = srid,
                    alias = parsed_query['fields'][field]['alias']
                ))
            else:
                fields.append('{field} as "{alias}"'.format(
                    field = parsed_query['fields'][field]['fullname'],
                    alias = parsed_query['fields'][field]['alias']
                ))

        # From clause tables
        tables = [ '"' + parsed_query['resources'][r]['table'] + '" as ' + parsed_query['resources'][r]['alias'] for r in parsed_query['resources']]

        # Where clause
        if len(parsed_query['filters']) > 0:
            for filter_tuple in parsed_query['filters']:
                wheres.append(filter_tuple[0])
                values += filter_tuple[1:]

        if len(wheres) > 0:
            where_clause = u'where ' + u' AND '.join(wheres)

        # Order by clause
        if len(parsed_query['sort']) > 0:
            orderby_clause = u'order by ' +u', '.join(parsed_query['sort'])

        # Build SQL
        sql = "select distinct {fields} from {tables} {where} {orderby} limit {limit} offset {offset};".format(
            fields = u','.join(fields),
            tables = u','.join(tables),
            where = where_clause,
            orderby = orderby_clause,
            limit = limit,
            offset = offset
        )

        # Execute query and aggregate execution time
        start_time = time.time()

        command_timeout = max(int(timeout - (context['elapsed_time'] * 1000)), 1000)

        connection_data.execute(u'SET LOCAL statement_timeout TO {0};'.format(command_timeout))
        records = connection_data.execute(sql, values)

        elapsed_time = min((time.time() - start_time), 1)
        context['elapsed_time'] = context['elapsed_time'] + elapsed_time

        if context['elapsed_time'] >= (config[CONFIG_SQL_TIMEOUT] / 1000):
            raise DataException(u'Execution timeout has expired. Current timeout value is {timeout} seconds.'.format(
                timeout = (config[CONFIG_SQL_TIMEOUT] / 1000)
            ))

        if output_format == QUERY_FORMAT_GEOJSON:
            # Add GeoJSON records
            feature_id = 0
            for r in records:
                feature_id += 1
                feature = {
                    'id' : feature_id,
                    'properties': {},
                    'geometry': None,
                    'type': 'Feature'
                }
                for field in parsed_query['fields'].keys():
                    if parsed_query['fields'][field]['is_geom']:
                        feature['geometry'] = shapely.wkb.loads(r[field].decode("hex"))
                    else:
                        feature['properties'][field] = r[field]
                result.append(feature)
        else:
            # Add flat json records
            for r in records:
                record = {}
                for field in parsed_query['fields'].keys():
                    if parsed_query['fields'][field]['is_geom']:
                        record[field] = shapely.wkb.loads(r[field].decode("hex"))
                    else:
                        record[field] = r[field]
                result.append(record)

        return result

    def _create_filter(self, metadata, mapping, f):
        if not type(f) is dict:
            raise DataException('Filter must be a dictionary.')

        if not 'operator' in f:
            raise DataException('Parameter operator is missing from filter.')

        if not f['operator'] in ALL_OPERATORS:
            raise DataException('Operator {operator} is not supported.'.format(operator = f['operator']))

        if not 'arguments' in f:
            raise DataException('Parameter arguments is missing from filter.')

        if not type(f['arguments']) is list or len(f['arguments']) == 0:
            raise DataException('Parameter arguments must be a list with at least one member.')

        try:
            if f['operator'] in COMPARE_OPERATORS:
                index = COMPARE_OPERATORS.index(f['operator'])
                return self._create_filter_compare(metadata, mapping, f, f['operator'], COMPARE_EXPRESSIONS[index])

            if f['operator'] in SPATIAL_OPERATORS:
                return self._create_filter_spatial(metadata, mapping, f, f['operator'])

        except ValueError as ex:
            log.error(ex)
            raise DataException('Failed to parse argument value for operator {operator}.'.format(operator = f['operator']))

        return None

    def _create_filter_compare(self, metadata, mapping, f, operator, expression):
        if len(f['arguments']) != 2:
            raise DataException('Operator {operator} expects two arguments.'.format(operator = operator))

        arg1 = f['arguments'][0]
        arg2 = f['arguments'][1]

        arg1_is_field = self._is_field(metadata, mapping, arg1)
        arg1_type = None
        if arg1_is_field:
            arg1_type = self._get_field_type(metadata, mapping, arg1)

        arg2_is_field = self._is_field(metadata, mapping, arg2)
        arg2_type = None
        if arg2_is_field:
            arg2_type = self._get_field_type(metadata, mapping, arg2)

        arg1_is_field_geom = self._is_field_geom(metadata, mapping, arg1)
        arg2_is_field_geom = self._is_field_geom(metadata, mapping, arg2)

        if arg1_is_field_geom or arg2_is_field_geom:
            raise DataException('Operator {operator} does not support geometry types.'.format(operator = operator))

        if arg1_is_field and arg2_is_field:
            if operator == OP_LIKE:
                raise DataException('Operator {operator} does not support two fields as arguments.'.format(operator = operator))

            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )
            aliased_arg2 = '{table}."{field}"'.format(
                table = metadata[mapping[arg2['resource']]]['alias'],
                field = arg2['name']
            )
            return ('(' + aliased_arg1 + ' ' + expression + ' ' + aliased_arg2 + ')',)
        elif arg1_is_field and not arg2_is_field:
            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )
            convert_to = ''

            if operator == OP_LIKE:
                if arg1_type != 'varchar':
                    raise DataException('Operator {operator} only supports text fields.'.format(operator = operator))

                arg2 = u'%' + unicode(arg2) + u'%'
            else:
                if arg1_type == 'varchar' and isinstance(arg2, numbers.Number):
                    if isinstance(arg2, int):
                        convert_to = '::int'
                    if isinstance(arg2, float):
                        convert_to = '::float'


            return ('(' +aliased_arg1 + convert_to + ' ' + expression + ' %s)', arg2)
        elif not arg1_is_field and arg2_is_field:
            aliased_arg2 = '{table}."{field}"'.format(
                table = metadata[mapping[arg2['resource']]]['alias'],
                field = arg2['name']
            )
            convert_to = ''

            if operator == OP_LIKE:
                if arg2_type != 'varchar':
                    raise DataException('Operator {operator} only supports text fields.'.format(operator = operator))

                arg1 = u'%' + unicode(arg1) + u'%'
            else:
                if arg2_type == 'varchar' and isinstance(arg1, numbers.Number):
                    if isinstance(arg1, int):
                        convert_to = '::int'
                    if isinstance(arg1, float):
                        convert_to = '::float'

            return ('(' + aliased_arg2 + convert_to  + ' ' + expression + ' %s)', arg1)
        else:
            if operator == OP_LIKE:
                raise DataException('Operator {operator} does not support two fields as literals.'.format(operator = operator))

            return ('(%s ' + expression + ' %s)', arg1, arg2)

    def _create_filter_spatial(self, metadata, mapping, f, operator):
        if operator == OP_AREA:
            if len(f['arguments']) != 3:
                raise DataException('Operator {operator} expects three arguments.'.format(operator = operator))
            return self._create_filter_spatial_area(metadata, mapping, f, operator)
        elif operator == OP_DISTANCE:
            if len(f['arguments']) != 4:
                raise DataException('Operator {operator} expects four arguments.'.format(operator = operator))
            return self._create_filter_spatial_distance(metadata, mapping, f, operator)
        elif operator == OP_CONTAINS:
            if len(f['arguments']) != 2:
                raise DataException('Operator {operator} expects two.'.format(operator = operator))
            return self._create_filter_spatial_relation(metadata, mapping, f, operator, 'ST_Contains')
        elif operator == OP_INTERSECTS:
            if len(f['arguments']) != 2:
                raise DataException('Operator {operator} expects two arguments.'.format(operator = operator))
            return self._create_filter_spatial_relation(metadata, mapping, f, operator, 'ST_Intersects')

    def _create_filter_spatial_area(self, metadata, mapping, f, operator):
        arg1 = f['arguments'][0]
        arg2 = f['arguments'][1]
        arg3 = f['arguments'][2]

        if arg2 in SPATIAL_COMPARE_OPERATORS:
            arg2 = COMPARE_EXPRESSIONS[COMPARE_OPERATORS.index(arg2)]
        else:
            raise DataException('Expression {expression} for operator {operator} is not valid.'.format(expression = arg2, operator = operator))

        arg1_is_field = self._is_field(metadata, mapping, arg1)
        arg1_srid = CRS_DEFAULT_DATABASE
        arg1_is_field_geom = self._is_field_geom(metadata, mapping, arg1)
        if arg1_is_field_geom:
            arg1_srid = self._get_field_srid(metadata, mapping, arg1)
        arg1_is_geom = self._is_geom(metadata, arg1)

        if not arg1_is_field_geom and not arg1_is_geom:
            raise DataException('First argument for operator {operator} must be a geometry field or a GeoJSON encoded geometry.'.format(operator = operator))

        if not isinstance(arg3, numbers.Number):
            raise DataException('Third argument for operator {operator} must be number.'.format(operator = operator))

        if arg1_is_field_geom:
            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )

            if arg1_srid != CRS_DEFAULT_DATABASE:
                aliased_arg1 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg1,
                srid = CRS_DEFAULT_DATABASE
            )

            return ('(ST_Area(' + aliased_arg1 + ') ' + arg2 + ' %s)', arg3)
        else:
            return ('(ST_Area(ST_GeomFromText(%s, 3857)) ' + arg2 + ' %s)', shapely.wkt.dumps(arg1), arg3)

    def _create_filter_spatial_distance(self, metadata, mapping, f, operator):
        arg1 = f['arguments'][0]
        arg2 = f['arguments'][1]
        arg3 = f['arguments'][2]
        arg4 = f['arguments'][3]

        if arg3 in SPATIAL_COMPARE_OPERATORS:
            arg3 = COMPARE_EXPRESSIONS[COMPARE_OPERATORS.index(arg3)]
        else:
            raise DataException('Expression {expression} for operator {operator} is not valid.'.format(expression = arg3, operator = operator))

        arg1_is_field = self._is_field(metadata, mapping, arg1)
        arg2_is_field = self._is_field(metadata, mapping, arg2)

        arg1_srid = CRS_DEFAULT_DATABASE
        arg2_srid = CRS_DEFAULT_DATABASE

        arg1_is_field_geom = self._is_field_geom(metadata, mapping, arg1)
        if arg1_is_field_geom:
            arg1_srid = self._get_field_srid(metadata, mapping, arg1)

        arg2_is_field_geom = self._is_field_geom(metadata, mapping, arg2)
        if arg2_is_field_geom:
            arg2_srid = self._get_field_srid(metadata, mapping, arg2)

        arg1_is_geom = self._is_geom(metadata, arg1)
        arg2_is_geom = self._is_geom(metadata, arg2)

        if not arg1_is_field_geom and not arg1_is_geom:
            raise DataException('First argument for operator {operator} must be a geometry field or a GeoJSON encoded geometry.'.format(operator = OP_DISTANCE))

        if not arg2_is_field_geom and not arg2_is_geom:
            raise DataException('Second argument for operator {operator} must be a geometry field or a GeoJSON encoded geometry.'.format(operator = OP_DISTANCE))

        if not isinstance(arg4, numbers.Number):
            raise DataException('Third argument for operator {operator} must be number.'.format(operator = OP_DISTANCE))

        if arg1_is_field_geom and arg2_is_field_geom:
            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )
            if arg1_srid != CRS_DEFAULT_DATABASE:
                aliased_arg1 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg1,
                srid = CRS_DEFAULT_DATABASE
            )

            aliased_arg2 = '{table}."{field}"'.format(
                table = metadata[mapping[arg2['resource']]]['alias'],
                field = arg2['name']
            )
            if arg2_srid != CRS_DEFAULT_DATABASE:
                aliased_arg2 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg2,
                srid = CRS_DEFAULT_DATABASE
            )
            return ('(ST_Distance(' + aliased_arg1 + ',' + aliased_arg2 + ') ' + arg3 + ' %s)', arg4)
        elif arg1_is_field_geom and not arg2_is_field_geom:
            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )
            if arg1_srid != CRS_DEFAULT_DATABASE:
                aliased_arg1 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg1,
                srid = CRS_DEFAULT_DATABASE
            )
            return ('(ST_Distance(' + aliased_arg1 + ', ST_Transform(ST_GeomFromText(%s, 3857), ' + str(CRS_DEFAULT_DATABASE) + ')) ' + arg3 + ' %s)', shapely.wkt.dumps(arg2), arg4)
        elif not arg1_is_field_geom and arg2_is_field_geom:
            aliased_arg2 = '{table}."{field}"'.format(
                table = metadata[mapping[arg2['resource']]]['alias'],
                field = arg2['name']
            )
            if arg2_srid != CRS_DEFAULT_DATABASE:
                aliased_arg2 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg2,
                srid = CRS_DEFAULT_DATABASE
            )
            return ('(ST_Distance(' + aliased_arg2 + ', ST_Transform(ST_GeomFromText(%s, 3857), ' +
                    str(CRS_DEFAULT_DATABASE) + ')) ' + arg3 + ' %s)', shapely.wkt.dumps(arg1), arg4)
        else:
            return ('(ST_Distance(ST_Transform(ST_GeomFromText(%s, 3857), ' +
                    str(CRS_DEFAULT_DATABASE) +
                    '), ST_Transform(ST_GeomFromText(%s, 3857), ' +
                    str(CRS_DEFAULT_DATABASE) + ')) ' + arg3 + ' %s)', shapely.wkt.dumps(arg1), shapely.wkt.dumps(arg2), arg4)

    def _create_filter_spatial_relation(self, metadata, mapping, f, operator, spatial_operator):
        arg1 = f['arguments'][0]
        arg2 = f['arguments'][1]

        arg1_is_field = self._is_field(metadata, mapping, arg1)
        arg2_is_field = self._is_field(metadata, mapping, arg2)

        arg1_srid = CRS_DEFAULT_DATABASE
        arg2_srid = CRS_DEFAULT_DATABASE

        arg1_is_field_geom = self._is_field_geom(metadata, mapping, arg1)
        if arg1_is_field_geom:
            arg1_srid = self._get_field_srid(metadata, mapping, arg1)

        arg2_is_field_geom = self._is_field_geom(metadata, mapping, arg2)
        if arg2_is_field_geom:
            arg2_srid = self._get_field_srid(metadata, mapping, arg2)

        arg1_is_geom = self._is_geom(metadata, arg1)
        arg2_is_geom = self._is_geom(metadata, arg2)

        if not arg1_is_field_geom and not arg1_is_geom:
            raise DataException('First argument for operator {operator} must be a geometry field or a GeoJSON encoded geometry.'.format(operator = OP_DISTANCE))

        if not arg2_is_field_geom and not arg2_is_geom:
            raise DataException('Second argument for operator {operator} must be a geometry field or a GeoJSON encoded geometry.'.format(operator = OP_DISTANCE))

        if arg1_is_field_geom and arg2_is_field_geom:
            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )
            if arg1_srid != CRS_DEFAULT_DATABASE:
                aliased_arg1 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg1,
                srid = CRS_DEFAULT_DATABASE
            )

            aliased_arg2 = '{table}."{field}"'.format(
                table = metadata[mapping[arg2['resource']]]['alias'],
                field = arg2['name']
            )
            if arg2_srid != CRS_DEFAULT_DATABASE:
                aliased_arg2 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg2,
                srid = CRS_DEFAULT_DATABASE
            )
            return ('(' + spatial_operator +'(' + aliased_arg1 + ',' + aliased_arg2 + ') = TRUE)', )
        elif arg1_is_field_geom and not arg2_is_field_geom:
            aliased_arg1 = '{table}."{field}"'.format(
                table = metadata[mapping[arg1['resource']]]['alias'],
                field = arg1['name']
            )
            if arg1_srid != CRS_DEFAULT_DATABASE:
                aliased_arg1 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg1,
                srid = CRS_DEFAULT_DATABASE
            )
            return ('(' + spatial_operator +'(' + aliased_arg1 + ', ST_Transform(ST_GeomFromText(%s, 3857), ' + str(CRS_DEFAULT_DATABASE) + ')) = TRUE)', shapely.wkt.dumps(arg2))
        elif not arg1_is_field_geom and arg2_is_field_geom:
            aliased_arg2 = '{table}."{field}"'.format(
                table = metadata[mapping[arg2['resource']]]['alias'],
                field = arg2['name']
            )
            if arg2_srid != CRS_DEFAULT_DATABASE:
                aliased_arg2 = 'ST_Transform({field}, {srid})'.format(
                field = aliased_arg2,
                srid = CRS_DEFAULT_DATABASE
            )
            return ('(' + spatial_operator +'(ST_Transform(ST_GeomFromText(%s, 3857), ' +
                    str(CRS_DEFAULT_DATABASE) +
                    '), ' + aliased_arg2 + ') = TRUE)', shapely.wkt.dumps(arg1))
        else:
            return ('(' + spatial_operator +'(ST_Transform(ST_GeomFromText(%s, 3857), ' +
                    str(CRS_DEFAULT_DATABASE) +
                    '), ST_Transform(ST_GeomFromText(%s, 3857), ' +
                    str(CRS_DEFAULT_DATABASE) + '))  = TRUE)', shapely.wkt.dumps(arg1), shapely.wkt.dumps(arg2))

    def _is_field(self, metadata, mapping, f):
        if f is None:
            return False

        if not type(f) is dict:
            return False

        if not 'name' in f:
            return False

        if 'resource' in f and (not f['resource'] in mapping or not mapping[f['resource']] in metadata):
            raise DataException('Resource {resource} does not exist.'.format(resource = f['resource']))

        # Set default resource of arguments if not already set
        if not 'resource' in f:
            resources = self._get_resources_by_field_name(metadata, f['name'])
            if len(resources) == 0:
                raise DataException(u'Field {field} does not exist.'.format(
                    field = f['name']
                ))
            elif len(resources) == 1:
                f['resource'] = resources[0]
            else:
                raise DataException(u'Field {field} is ambiguous for resources {resources}.'.format(
                    field = f['name'],
                    resources = u','.join(resources)
                ))

        if not f['name'] in metadata[mapping[f['resource']]]['fields']:
            raise DataException('Field {field} does not belong to resource {resource}.'.format(field = f['name'], resource = f['resource']))

        return True

    def _get_field_type(self, metadata, mapping, f):
        if not self._is_field(metadata, mapping, f):
            return None

        return metadata[mapping[f['resource']]]['fields'][f['name']]['type']

    def _is_field_geom(self, metadata, mapping, f):
        if not self._is_field(metadata, mapping, f):
            return False

        if f['name'] == metadata[mapping[f['resource']]]['geometry_column']:
            return True

        return False

    def _get_field_srid(self, metadata, mapping, f):
        if not self._is_field_geom(metadata, mapping, f):
            return None

        return metadata[mapping[f['resource']]]['srid']

    def _is_geom(self, metadata, f):
        return isinstance(f, shapely.geometry.base.BaseGeometry)

    def _get_resources_by_field_name(self, metadata, field):
        resources = []

        for resource in metadata:
            if field in metadata[resource]['fields']:
                resources.append(resource)

        return resources

    def getResources(self, config, connection=None):
        engine = None
        auto_close = False

        resources = None
        result = {}

        try:
            if connection is None:
                auto_close = True
                engine = create_engine(config[CONFIG_SQL_CATALOG], echo=False)
                connection = engine.connect()

            sql = u"""
                    select  resource_db.resource_id as db_resource_id,
                            package_revision.title as package_title,
                            package_revision.notes as package_notes,
                            resource_db.resource_name as resource_name,
                            resource_wms.resource_id as wms_resource_id,
                            resource_db.geometry_type as geometry_type,
                            resource_wms.wms_server as wms_server,
                            resource_wms.wms_layer as wms_layer
                    from
                        (
                        select  id as resource_id,
                                json_extract_path_text((extras::json),'vectorstorer_resource') as vector_storer,
                                json_extract_path_text((extras::json),'geometry') as geometry_type,
                                json_extract_path_text((extras::json),'parent_resource_id') as resource_parent_id,
                                resource_group_id as group_id,
                                name as resource_name
                        from	resource_revision
                        where	format = 'data_table'
                                and current = True
                                and state = 'active'
                                and json_extract_path_text((extras::json),'vectorstorer_resource')  = 'True'
                        ) as resource_db
                        left outer join
                            (
                            select	id as resource_id,
                                    json_extract_path_text((extras::json),'vectorstorer_resource') as vector_storer,
                                    json_extract_path_text((extras::json),'geometry') as ggeometry_type,
                                    json_extract_path_text((extras::json),'parent_resource_id') as resource_parent_id,
                                    resource_group_id as group_id,
                                    json_extract_path_text((extras::json),'wms_server') as wms_server,
                                    json_extract_path_text((extras::json),'wms_layer') as wms_layer
                            from	resource_revision
                            where	format = 'wms'
                                    and current = True
                                    and state = 'active'
                                    and json_extract_path_text((extras::json),'vectorstorer_resource')  = 'True'
                            ) as resource_wms
                                on	resource_db.group_id = resource_wms.group_id
                                    and resource_db.resource_id = resource_wms.resource_parent_id
                        left outer join resource_group_revision
                                on	resource_group_revision.id = resource_db.group_id
                                    and resource_group_revision.state = 'active'
                                    and resource_group_revision.current = True
                        left outer join	package_revision
                                on	resource_group_revision.package_id = package_revision.id
                                    and package_revision.state = 'active'
                                    and package_revision.current = True;
            """

            resources = connection.execute(sql)
            for resource in resources:
                result[resource['db_resource_id']] = {
                    'table': resource['db_resource_id'],
                    'resource_name' : resource['resource_name'],
                    'package_title' : resource['package_title'],
                    'package_notes' : resource['package_notes'],
                    'wms': None if resource['wms_resource_id'] is None else resource['wms_resource_id'],
                    'wms_server': None if resource['wms_server'] is None else resource['wms_server'],
                    'wms_layer': None if resource['wms_layer'] is None else resource['wms_layer'],
                    'geometry_type': resource['geometry_type']
                }
        finally:
            if not resources is None:
                resources.close()
            if not connection is None and auto_close:
                connection.close()

        return result

    def describeResource(self, config, connection=None, id=None):
        engine = None
        auto_close = False

        result = {}
        srid = None
        geometry_column = None

        try:
            if connection is None:
                auto_close = True
                engine = create_engine(config[CONFIG_SQL_DATA], echo=False)
                connection = engine.connect()

            sql = text(u"""
                SELECT	attname::varchar as "name",
	                    pg_type.typname::varchar as "type",
    	                pg_attribute.attnum as "position",
    	                geometry_columns.srid as srid
                FROM	pg_class
	    	                inner join pg_attribute
	    		                on pg_attribute.attrelid = pg_class.oid
	    	                inner join pg_type
	    		                on pg_attribute.atttypid = pg_type.oid
	    	                left outer join geometry_columns
	    		                on geometry_columns.f_table_name = pg_class.relname and
	    		                   pg_type.typname = 'geometry'
                WHERE	pg_attribute.attisdropped = False and
    	                pg_class.relname = :resource and
    	                pg_attribute.attnum > 0
            """)

            fields = connection.execute(sql, resource = id).fetchall()
            for field in fields:
                if field['name'].decode('utf-8').startswith('_'):
                    continue

                result[field['name'].decode('utf-8')] = {
                    'name': field['name'].decode('utf-8'),
                    'type': field['type'].decode('utf-8')
                }

                if not field['srid'] is None:
                    if not srid is None:
                        raise DataException('More than 1 geometry columns found in resource {id}'.format(id = id))

                    geometry_column = field['name'].decode('utf-8')
                    srid = field['srid']
        finally:
            if not connection is None and auto_close:
                connection.close()

        return {
            "id": id,
            "fields" : result,
            "srid": srid,
            "geometry_column" : geometry_column
        }
