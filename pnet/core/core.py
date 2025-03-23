from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T


class Util:
    

    def __init__(self):
        self.__setting_file = 'config/setting.json'
        self.spark = self.__create_spark_session()
        self.config = self.config()


    def config(self):
        """
            Load and return the configuration settings from the predefined settings file.
        
            Returns:
                dict: A dictionary containing the configuration settings loaded from the JSON file.
        """

        import json

        with open(self.__setting_file, 'r') as file:
            return json.load(file)


    def __create_spark_session(self):
        spark = (SparkSession.builder.appName("pnet")
                 .config("dfs.client.read.shortcircuit.skip.checksum", "true")
                 .getOrCreate()
                )
        
        spark.sparkContext.setCheckpointDir("/code/tmp/Checkpoint/")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


        return spark


    def load_data(self, file_path, _format = 'json'):

        _schema = T.StructType(
            [
                T.StructField("Unnamed: 0", T.StringType(), True),
                T.StructField("amt", T.DecimalType(10,2), True),
                T.StructField("category", T.StringType(), True),
                T.StructField("cc_bic", T.StringType(), True),
                T.StructField("cc_num", T.StringType(), True),
                T.StructField("is_fraud", T.StringType(), True),
                T.StructField("merch_eff_time", T.StringType(), True),
                T.StructField("merch_last_update_time", T.StringType(), True),
                T.StructField("merch_lat", T.StringType(), True),
                T.StructField("merch_long", T.StringType(), True),
                T.StructField("merch_zipcode", T.StringType(), True),
                T.StructField("merchant", T.StringType(), True),
                T.StructField("personal_detail", T.StringType(), True),
                T.StructField("trans_date_trans_time", T.StringType(), True),
                T.StructField("trans_num", T.StringType(), True), ]
        )
        
        df = self.spark.read.format('org.apache.spark.sql.json').schema(_schema).load(file_path)

        df.checkpoint()
        df.persist()

        return df


    def __store_ds_transaction(self):

        _timezone = self.config['config']['timezone']

        # load JSON dataset 
        # load transaction details
        #df_transaction = self.load_data("data/raw/cc_transaction.json")
        df_transaction = self.load_data("data/raw/sampling.json")

        df_transaction = df_transaction.na.fill('')
        df_transaction = df_transaction.withColumn("merch_eff_time", self.udf_parse_epoch(F.col("merch_eff_time"), F.lit(_timezone)))
        df_transaction = df_transaction.withColumn("merch_last_update_time", self.udf_parse_epoch(F.col("merch_last_update_time"), F.lit(_timezone)))
        df_transaction = df_transaction.withColumn("trans_date_trans_time", self.udf_parse_datetime(F.col("trans_date_trans_time"), F.lit(_timezone)))
        df_transaction = df_transaction.withColumn("merchant", self.udf_parse_merchant_name(F.col("merchant")))

        # save to parquet, except personal_detail
        (df_transaction
            .coalesce(1)
            .write.mode('overwrite')
            .parquet("/code/data/ingestion/transaction/") )

        return df_transaction


    def __store_ds_personal_detail(self):

        df_transaction = self.spark.read.parquet('/code/data/ingestion/transaction/')

        df_personal_detail = (df_transaction.select(
                F.col("trans_num"), 
                F.json_tuple(F.col("personal_detail"), "person_name", "gender", "address", "lat", "long", "city_pop","job","dob"))
                .toDF("trans_num", "person_name", "gender", "address", "lat", "long", "city_pop", "job", "dob")
            ).select( 
                F.col('*'),
                F.json_tuple(F.col("address"), "street", "city", "state", "zip")
            ).toDF("trans_num", "person_name", "gender", "address", "lat", "long", "city_pop", "job", "dob", "street", "city", "state", "zip")

        df_personal_detail = df_personal_detail.na.fill('')
        df_personal_detail = df_personal_detail.withColumn("first", self.udf_parse_name(F.col("person_name"), F.lit(0)))
        df_personal_detail = df_personal_detail.withColumn("last", self.udf_parse_name(F.col("person_name"), F.lit(1)))
        df_personal_detail = df_personal_detail.drop("person_name", "address")

        # save to parquet, except address
        (df_personal_detail
            .coalesce(1)
            .write.mode('overwrite')
            .parquet("/code/data/ingestion/personal/") )
        
        return df_personal_detail


    def generate_csv(self, masked_record = True):
        df_trans = self.__store_ds_transaction()
        df_trans.createOrReplaceTempView("tbl_trans")

        df_personal = self.__store_ds_personal_detail()
        df_personal.createOrReplaceTempView("tbl_personal")        

        _sql = "SELECT tbl_trans.* , tbl_personal.*  from tbl_trans INNER JOIN tbl_personal ON tbl_trans.trans_num = tbl_personal.trans_num"
        df = self.spark.sql(_sql)

        df = df.select(['`Unnamed: 0`', 'trans_date_trans_time', 'cc_num', 'merchant', 
                        'category', 'amt', 'first', 'last', 'gender', 'street', 'city', 
                        'state', 'zip', 'lat', 'long', 'city_pop', 'job', 'dob', 'tbl_personal.trans_num', 
                        'merch_lat', 'merch_long', 'is_fraud', 'merch_zipcode', 'merch_last_update_time', 
                        'merch_eff_time', 'cc_bic']
                    )
        
        df = df.na.replace('Null', None).replace('NA', None)
        df = df.na.fill('')


        if (masked_record):
            df = df.withColumn("cc_num", self.udf_mask_cc_num(F.col("cc_num")))

        # save CSV as part of requirement
        df.coalesce(1).write.mode('overwrite').options(header='True', delimiter='|').csv("/code/data/output/")

        (df.coalesce(1)
            .write.mode('overwrite')
            .parquet("/code/data/ingestion/final/") )

        self.spark.catalog.dropTempView("tbl_trans")
        self.spark.catalog.dropTempView("tbl_personal")

        return df


    @staticmethod
    @F.udf(returnType=T.StringType())
    def udf_parse_name(_name, _name_position ) -> str:
        import re

        _separator = ',|@|/'
        _result = ""

        try:
            _result = re.split(_separator, _name.replace("|",","))[_name_position]
            _result = re.sub('[^A-Za-z0-9]+', '', _result)
            return _result

        except Exception as e:
            return _result


    @staticmethod
    @F.udf(returnType=T.StringType())
    def udf_parse_datetime(_datestring, _timezone):
        from dateutil import parser
        import pytz 

        _result = ""
        try:
            _dt = parser.parse(_datestring)
            timezone = pytz.timezone(_timezone)
            _result = str(timezone.localize(_dt))
        except Exception as e:
            pass

        return _result  


    @staticmethod
    @F.udf(returnType=T.StringType())
    def udf_parse_epoch(_epoch, _timezone):
        import pytz 
        from datetime import datetime, timedelta

        _result = ""
        try:
            _dt = datetime.fromtimestamp(int(str(_epoch)[:10]))
            timezone = pytz.timezone(_timezone)
            _result = str(timezone.localize(_dt))
        except Exception as e:
            pass

        return _result    


    @staticmethod
    @F.udf(returnType=T.StringType())
    def udf_parse_merchant_name(_value):
    
        try:
            return _value.replace("fraud_", "")
        except Exception as e:
            return ""


    @staticmethod
    @F.udf(returnType=T.StringType())
    def udf_mask_cc_num(_value):
        return str(_value)[:6] + ('#' * (len(str(_value)) - 10)) + str(_value)[-4:]
        

