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
        StructField("src_port", StringType(), True),
        StructField("dest_port", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("dns", StructType([
            StructField("type", StringType(), True),  # answer
            StructField("rrname", StringType(), True),  # dns name
            StructField("grouped", StructType([
                StructField("CNAME", ArrayType(StringType()), True),
                StructField("A", ArrayType(StringType()), True)
            ]))
            # StructField("answers", StringType(), True)
            #StructField("answers", ArrayType(StructType([
            #    StructField("rrname", StringType(), True),
            #    StructField("ttl", StringType(), True),
            #    StructField("rrtype", StringType(), True),  # A
            #    StructField("rdata", StringType(), True)
            #])))
        ]))
    ])

    #"to_json(struct(*)) AS value"
    suricata_log = spark.readStream.\
        format("kafka").\
        option("kafka.bootstrap.servers", "192.168.2.12:9092").\
        option("subscribe", "suricata"). \
        option("kafkaConsumer.pollTimeoutMs", "600000"). \
        option("failOnDataLoss", False).\
        load()

    # .selectExpr("data.src_ip", "data.event_type", "data.dns.rrname", "data.dns.type", "cast (timestamps as string)") \
    # .where('src_ip like "%.20.62%"') \
    suricata_mod_log = suricata_log \
        .selectExpr("cast (value as string) as json", "cast (timestamp as timestamp) as timestamps")\
        .select(from_json("json", schema).alias("data"), "timestamps") \
        .selectExpr("data.src_ip as src_ip",
                    "data.event_type as event_type",
                    "data.dns.rrname as rrname",
                    "data.dns.type as type",
                    "data.dns.grouped.CNAME[0] as cname",
                    "data.dns.grouped.A[0] as cdata",
                    "timestamps as ts") \
        .where('event_type = "dns" and type = "answer" and cname is not null and src_ip in ("61.82.88.6", "8.8.8.8") and cdata is not null') \
        .groupBy('rrname', window('ts', '60 minutes'))\
        .agg(count('rrname').alias("rrname_cnt"), max('ts').alias("timestamps"), max("cname"), max("cdata")) \
        .orderBy("timestamps", ascending=False) \
        # .selectExpr("to_json(struct(*)) as value")
    ## withWatermark("timestamp", "10 minutes")
    #.where('type = "answer"') 
       # .where('cname is not null') 
       # .where('src_ip in ("61.82.88.6", "8.8.8.8")') 
       # .where('cdata is not null') 

    # suricata_mod_log = suricata_log.withWatermark("timestamp", "10 minutes") \
    #     .selectExpr("cast (value as string) as json") \
    #     .select(from_json("json", schema).alias("data")) \
    #     .selectExpr("data.src_ip as src_ip", "data.event_type as event_typ", "data.dns.rrname as dns",
    #                 "data.dns.type as type", "data.timestamp as timestamps") \
    #     .where('type = "query"' and 'src_ip like "%192.168.2%"' and 'dns is not null') \
    #     .groupBy(window('timestamps', '60 minutes', '30 minutes'), 'dns') \
    #     .agg(count('dns'), max('timestamps'), max('src_ip')) \
    #     .orderBy("count(dns)", ascending=False)

    #suricata_mod_log.createOrReplaceTempView('tb')
    #suricata_sql_log = spark.sql("""
    #                    select *
    #                    from tb as t
    #                """)

    # suricata_sql_log = spark.sql("""
    #                     select (t.rrname, count(t.rrname), max(t.timestamps)) as value
    #                     from tb as t
    #                     where t.type = "query" and t.src_ip like "%.20.62%"
    #                     GROUP BY t.rrname
    #                     ORDER BY count(t.rrname) DESC
    #                 """).select(to_json("value").alias("value"))
    # ORDER BY DNS_CNT DESC

    query = suricata_mod_log.writeStream.\
        outputMode('complete').\
        option('numRows', 50). \
        option('truncate', False). \
        format('console'). \
        start()

    # query = suricata_mod_log.writeStream.format("kafka") \
    #     .option("kafka.bootstrap.servers", "192.168.2.12:9092") \
    #     .option("topic", "suricata12") \
    #     .option("checkpointLocation", "/home/tmp/check_dhcp3") \
    #     .outputMode('complete') \
    #     ..trigger(continuous="1 second") \
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

