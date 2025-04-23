from dataclasses import dataclass
from datetime import time


class Assignment:
    def __init__(self, assignment_id: int, student_id: int, stop_id: int,
                 bus_id: int, pickup_time: time):
        self.assignment_id = assignment_id
        self.student_id = student_id
        self.stop_id = stop_id
        self.bus_id = bus_id
        self.pickup_time = pickup_time


class BusStop:
    def __init__(self, stop_id: int, name: str, latitude: float, longitude: float):
        self.stop_id = stop_id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude


@dataclass
class Point:
    point_id: str
    name: str
    address: str
    latitude: float
    longitude: float
    amenity: str  # Thay type bằng amenity vì đây là trường phổ biến trong OSM

    @classmethod
    def from_geojson_feature(cls, feature: dict[str, any]) -> 'Point':
        """
        Tạo đối tượng Point từ một feature trong GeoJSON
        """
        properties = feature['properties']
        geometry = feature['geometry']
        coordinates = geometry['coordinates']  # [longitude, latitude]

        return cls(
            point_id=str(properties.get('osm_id', '')),  # OSM ID thường là định danh duy nhất
            name=properties.get('name', ''),
            address=properties.get('addr:street', ''),  # Hoặc có thể kết hợp nhiều trường địa chỉ
            longitude=coordinates[0],
            latitude=coordinates[1],
            amenity=properties.get('amenity', '')
        )


@dataclass
class Road:
    geometry: any  # Geometry từ GeoJSON
    coordinates: list[tuple[float, float]]  # Thêm coordinates
    name: str
    oneway: bool
    highway: str
    maxspeed: float = None
    lanes: int = None
    bridge: bool = False
    tunnel: bool = False

    @classmethod
    def from_geojson_feature(cls, feature: dict) -> 'Road':
        properties = feature['properties']

        # Lấy coordinates từ geometry
        geometry = feature['geometry']
        coordinates = []
        if geometry['type'] == 'LineString':
            coordinates = geometry['coordinates']
        elif geometry['type'] == 'MultiLineString':
            # Nếu là MultiLineString, lấy coordinates từ linestring đầu tiên
            coordinates = geometry['coordinates'][0]

        # Xử lý maxspeed
        maxspeed = properties.get('maxspeed')
        if maxspeed is not None:
            try:
                maxspeed = float(maxspeed)
            except (ValueError, TypeError):
                maxspeed = None

        # Xử lý lanes
        lanes = properties.get('lanes')
        if lanes is not None:
            try:
                lanes = int(lanes)
            except (ValueError, TypeError):
                lanes = None

        return cls(
            geometry=geometry,
            coordinates=coordinates,  # Thêm coordinates vào constructor
            name=properties.get('name', ''),
            oneway=properties.get('oneway', False),
            highway=properties.get('highway', ''),
            maxspeed=maxspeed,
            lanes=lanes,
            bridge=properties.get('bridge', False),
            tunnel=properties.get('tunnel', False)
        )


class Student:
    def __init__(self, student_id: int, longitude: float, latitude: float, name: str, address: str):
        self.student_id = student_id
        self.name = name
        self.address = address
        self.latitude = latitude
        self.longitude = longitude


class Bus:
    def __init__(self, bus_id: int, longitude: float, latitude: float, capacity: str):
        self.bus_id = bus_id
        self.capacity = capacity
        self.latitude = latitude
        self.longitude = longitude
