from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T

from pnet.core import core

class Analysis:

    def __init__(self):
        pass


    def get_enrich_dataset(self, masked_record = True):

        _util = core.Util()

        # get information from RAW json file (with )
        #df = _util.generate_csv(masked_record)
        df = _util.spark.read.parquet("/code/data/ingestion/final/")
        df = df.withColumn("card_type", self.udf_enrich_card_type(F.col("cc_num")))

        df.checkpoint()
        df.persist()

        return df


    def plot_map_for_fraud_transaction(self, df):
        import folium
        from folium.plugins import MarkerCluster


        map = folium.Map(location=[39.492341, -78.859114], zoom_start=6)
        marker_cluster = MarkerCluster().add_to(map)

        for row in df.filter("is_fraud = 1").rdd.toLocalIterator():

            folium.Marker(
                location= [ row['merch_lat'], row['merch_long'] ],
                popup=row['merchant'],
                icon=folium.Icon(color="red", icon="fraud"),
            ).add_to(marker_cluster)

            folium.Marker(
                location= [ row['lat'], row['long'] ],
                icon=folium.Icon(color="green", icon="fraud"),
            ).add_to(marker_cluster)

            coords = [
                [ float(row['merch_lat']), float(row['merch_long']) ],
                [ float(row['lat']), float(row['long']) ] ]

            folium.plugins.PolyLineOffset(coords, weight=2, dash_array="5,10", color="black", opacity=1).add_to(marker_cluster)

        return map        


    @staticmethod
    @F.udf(returnType=T.StringType())
    def udf_enrich_card_type(number):
        import re

        number = str(number).replace("#","0")

        card_types = {
            "Visa": r"^4\d{12}(\d{3})?(\d{3})?$",
            "MasterCard": r"^(5[1-5]\d{4}|222[1-9]|22[3-9]\d{3}|2[3-6]\d{4}|27[01]\d{3}|2720)\d{10}$",
            "American Express": r"^3[47]\d{13}$",
            "Discover": r"^6(?:011|5\d{2}|4[4-9]\d|22[1-9]\d{1,2})\d{12}$",
            "Diners Club": r"^3(?:0[0-5]|[68]\d)\d{11}$",
            "JCB": r"^35(?:2[89]|[3-8]\d)\d{12}$",
            "Maestro": r"^(50|5[6-9]|6[0-9])\d{10,17}$"
        }

        for card_type, pattern in card_types.items():
            if re.match(pattern, number):
                return card_type

        return "Unknown"

