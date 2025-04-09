import json


def get_hanoi_roads_geojson(mode="cropped"):
    """
    :param mode: "cropped" or "full"
    :return:
    """
    with open(f"data/hanoi_roads_{mode}.geojson", "r") as f:
        return json.load(f)
