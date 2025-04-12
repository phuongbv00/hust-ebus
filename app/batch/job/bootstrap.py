import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString

from batch.core import Job
from deps.s3 import s3_upload
from deps.spark import spark_read_s3, spark_read_db, spark_write_db, get_spark_session


def _seed_std_addresses(mode="cropped"):
    """
    Seed student addresses
    :param mode: "cropped" or "full"
    """
    local_seed_filepath = f"data/hanoi_points_{mode}.csv"
    s3_key = "hanoi_points.csv"
    s3_upload(local_seed_filepath, s3_key)
    df = spark_read_s3(s3_key)
    df = df.withColumnRenamed("osm_id", "std_id")
    df = df.drop("osm_type", "geom_type")
    spark_write_db("std_address", df, "overwrite")
    query = """
        SELECT *
        FROM std_address
    """
    df = spark_read_db(query)
    df.show()


def _seed_roads():
    # Start Spark session
    spark = get_spark_session()

    # Load GeoJSON
    gdf = gpd.read_file("data/hanoi_roads_full.geojson")

    # Clean and prepare base attributes
    gdf["road_id"] = gdf.get("osm_id", gdf.index)
    gdf["name"] = gdf.get("name", "").fillna("Chưa có tên")

    # --------------------
    # Seed Roads table (road_id, name)
    # --------------------
    roads_pdf = gdf[["road_id", "name"]]
    roads_sdf = spark.createDataFrame(roads_pdf)
    spark_write_db("roads", roads_sdf, "overwrite")

    # --------------------
    # Seed RoadPoints table (road_id, longitude, latitude, order)
    # --------------------

    # Extract (road_id, lon, lat, order) from geometry
    def extract_coords_with_order(r):
        geom = r.geometry
        road_id = r.road_id
        coords = []
        if isinstance(geom, LineString):
            coords = list(geom.coords)
        elif isinstance(geom, MultiLineString):
            for line in geom.geoms:
                coords.extend(line.coords)
        return [(road_id, lon, lat, idx) for idx, (lon, lat) in enumerate(coords)]

    # Explode geometry to points
    coords_data = []
    for _, row in gdf.iterrows():
        coords_data.extend(extract_coords_with_order(row))

    # Create DataFrame
    points_df = pd.DataFrame(coords_data, columns=["road_id", "longitude", "latitude", "order"])
    points_sdf = spark.createDataFrame(points_df)

    # Write to Spark table
    spark_write_db("road_points", points_sdf, "overwrite")


class BootstrapJob(Job):
    def run(self, *args, **kwargs):
        _seed_std_addresses()
        _seed_roads()
