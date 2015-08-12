import logging

import json
import geojson

import shapely.geometry
import shapely.geometry.base

log = logging.getLogger(__name__)

class ShapelyJsonDecoder(json.JSONDecoder):
    def decode(self, json_string):   
        def shapely_object_hook(obj):
            if 'coordinates' in obj and 'type' in obj:
                return shapely.geometry.shape(obj)
            return obj
        
        return json.loads(json_string, object_hook=shapely_object_hook)
