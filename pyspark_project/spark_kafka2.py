from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import json


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("src_ip", StringType(), True),
        StructField("dest_ip", StringType(), True),
        # StructField("src_port", StringType(), True),
        # StructField("dest_port", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("dhcp", StructType([
            StructField("type", StringType(), True), # request
            StructField("id", StringType(), True), # id
            StructField("client_mac", StringType(), True), # group
            StructField("hostname", StringType(), True), # hostname
            StructField("client_ip", StringType(), True),
            StructField("assigned_ip", StringType(), True),
            StructField("requested_ip", StringType(), True)
        ]))
    ])

    #"to_json(struct(*)) AS value"
    suricata_log = spark.readStream.\
        format("kafka").\
        option("kafka.bootstrap.servers", "192.168.2.12:9092").\
        option("subscribe", "suricata"). \
        option("kafkaConsumer.pollTimeoutMs", "6000000000000000"). \
        option("failOnDataLoss", False).\
        load()

    ## withWatermark("timestamp", "10 minutes")
    suricata_sql_log = suricata_log \
        .selectExpr("cast (value as string) as json", "cast (timestamp as timestamp) as timestamps") \
        .select(from_json("json", schema).alias("data"), "timestamps") \
        .selectExpr("data.src_ip as src_ip",
                    "data.event_type as event_type",
                    "data.dest_ip as dest_ip",
                    "data.timestamp as timestamp",
                    "data.dhcp.type as type",
                    "data.dhcp.id as id",
                    "data.dhcp.client_mac as mac",
                    "data.dhcp.client_ip as cip",
                    "data.dhcp.hostname as hostname",
                    "data.dhcp.assigned_ip as aip",
                    "data.dhcp.requested_ip as rip",
                    "timestamps as ts") \
        .where('event_type = "dhcp" and src_ip != "61.82.88.6" and hostname is not null') \
        .groupBy('id' ,window('ts', '30 minutes')) \
        .agg(max('src_ip'), max('hostname'), max('cip'), max('rip'), max('ts'), count('id')) \
        .orderBy("max(ts)", ascending=False)
        #.selectExpr("to_json(struct(*)) as value")
        #.selectExpr("count(mac) as mac",
        #            "max(type) as type", 
        #            "max(src_ip) as src_ip", 
        #            "max(hostname) as hostname", 
        #            "max(cip) as cip")
        #.selectExpr("to_json(struct(*)) as value")

        # .withWatermark("ts", "30 minutes") \
        # .groupBy('rrname', window('ts', '30 minutes', '30 minutes')) \
        # .agg(count('rrname'), max('ts')) \
        # .orderBy("count(rrname)", ascending=False) \
        # .selectExpr("to_json(struct(*)) as value")

    # suricata_mod_log = suricata_log.\
    #     withWatermark("timestamp", "5 minutes").\
    #     selectExpr("cast (value as string) as json", "cast (timestamp as timestamp) as timestamps")\
    #     .select(from_json("json", schema).alias("data"), "timestamps") \
    #     .selectExpr("data.src_ip", "data.event_type", "data.dns.rrname", "data.dns.type", "cast (timestamps as string)")
    #     # .groupBy('src_ip', 'rrname', 'event_type')\
    #     # .agg(count('rrname'))\
    #     # .orderBy("count(rrname)", ascending=False)\
    #     # .where('src_ip = "192.168.2.11"')\
    #     # .where('event_type = "dns"')
    #
    # suricata_mod_log.createOrReplaceTempView('tb')
    # suricata_sql_log = spark.sql("""select (t.rrname, count(t.rrname), max(t.timestamps)) as value
    #                 from tb as t
    #                 where t.type = "query" and t.src_ip like "192.168.%"
    #                 GROUP BY t.rrname
    #                 ORDER BY count(t.rrname) DESC
    #                 """)\
    #     .select(to_json("value").alias("value"))
    # ORDER BY DNS_CNT DESC

    # query = a.writeStream.outputMode('complete').format('console').start()

    query = suricata_sql_log.writeStream.\
        outputMode('complete').\
        option('numRows', 50). \
        option('truncate', False). \
        format('console').\
        start()
    #trigger(processingTime='3 seconds')
    #query = suricata_sql_log.writeStream.format("kafka") \
    #     .option("kafka.bootstrap.servers", "192.168.2.12:9092") \
    #     .option("topic", "suricata_dhcp") \
    #     .option("checkpointLocation", "/home/tmp/suricata_dhcp") \
    #     .outputMode('update') \
    #     .start()

    """Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

            Options include:

            * 'append':Only the new rows in the streaming DataFrame/Dataset will be written to
               the sink
            * 'complete':All the rows in the streaming DataFrame/Dataset will be written to the sink
               every time these is some updates
            * 'update':only the rows that were updated in the streaming DataFrame/Dataset will be
               written to the sink every time there are some updates. If the query doesn't contain
               aggregations, it will be equivalent to 'append' mode.

           .. note:: Evolving.

            >>> writer = sdf.writeStream.outputMode('append')
    """


    query.awaitTermination()
