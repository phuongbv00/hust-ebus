import numpy as np

EARTH_RADIUS_M = 6_371_000


def haversine_distance(lat1, lng1, lat2, lng2):
    """
    Tính khoảng cách Haversine giữa hai tọa độ địa lý (đơn vị: mét)
    """
    lat1, lng1, lat2, lng2 = map(np.radians, [lat1, lng1, lat2, lng2])
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return EARTH_RADIUS_M * c
