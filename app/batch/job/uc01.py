from batch.core import Job
from deps.biz import get_hanoi_roads_geojson


class UC01Job(Job):
    def run(self, *args, **kwargs):
        roads_geojson = get_hanoi_roads_geojson()
        print(roads_geojson)
