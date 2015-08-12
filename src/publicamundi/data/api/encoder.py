import logging

import json
import geojson

import shapely.geometry
import shapely.geometry.base

log = logging.getLogger(__name__)

class ShapelyJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, shapely.geometry.base.BaseGeometry):
            return shapely.geometry.mapping(obj)
        return json.JSONEncoder.default(self, obj)

class ShapelyGeoJsonEncoder(geojson.codec.GeoJSONEncoder):
    def default(self, obj):
        if isinstance(obj, shapely.geometry.base.BaseGeometry):
            return shapely.geometry.mapping(obj)
        return json.GeoJSONEncoder.default(self, obj)
