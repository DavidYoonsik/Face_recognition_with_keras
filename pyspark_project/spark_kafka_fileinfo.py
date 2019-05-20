from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import json

src_ip = '192.168.20.33'

if __name__ == "__main__":

    global src_ip

    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("src_ip", StringType(), True),
        StructField("dest_ip", StringType(), True),
        StructField("src_port", StringType(), True),
        StructField("dest_port", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("fileinfo", StructType([
            StructField("stored", StringType(), True),
            StructField("magic", StringType(), True),
            StructField("sha256", StringType(), True),
            StructField("filename", StringType(), True)
        ])),
        StructField("http", StructType([
            StructField("hostname", StringType(), True),
            StructField("url", StringType(), True)
        ]))
    ])

    #"to_json(struct(*)) AS value"
    suricata_log = spark.readStream.\
        format("kafka").\
        option("kafka.bootstrap.servers", "192.168.2.12:9092").\
        option("subscribe", "suricata"). \
        option("kafkaConsumer.pollTimeoutMs", 3600*1000*12). \
        option("failOnDataLoss", False).\
        load()

    # .selectExpr("data.src_ip", "data.event_type", "data.dns.rrname", "data.dns.type", "cast (timestamps as string)") \
    # .where('src_ip like "%.20.62%"') \
    suricata_mod_log = suricata_log \
        .selectExpr("cast (value as string) as json", "cast (timestamp as timestamp) as timestamps")\
        .select(from_json("json", schema).alias("data"), "timestamps") \
        .selectExpr("data.src_ip as src_ip", 
                    "data.dest_ip as dest_ip", 
                    "data.event_type as event_type", 
                    "data.fileinfo.stored as save", 
                    "data.fileinfo.sha256 as sha256",
                    "lower(data.fileinfo.magic) as magic", 
                    "data.fileinfo.filename as fname", 
                    "data.http.hostname as hname", 
                    "timestamps as ts") \
        .where('event_type = "fileinfo"') \
        .where(' magic like "%pdf%" or magic like "%micro%" or magic like "%zip%" or magic like "%sat%" or magic like "%execut%" ')\
        .groupBy('magic', window('ts', '60 minutes').alias('windows'))\
        .agg(count('fname').alias('fname_cnt'), max('fname').alias('file_name'), max("ts"), max("dest_ip").alias("dip"), max("hname").alias("host") ) \
        .orderBy("max(ts)", ascending=False)
# .where(' magic like "%pdf%" or magic like "%micro%" or magic like "%zip%" or magic like "%sat%" or magic like "%execut%" ')
        # .selectExpr('fname', 'magic', 'cast (windows.start as timestamp) as start', 'cast (windows.end as timestamp) as end', 'fname_cnt', 'magic_cnt', 'ts') \
        # .where('current_timestamp between start and end') \

        #         .where('lower(fname) like "%pdf%" or lower(fname) like "%exe%" or lower(fname) like "%msi%" or lower(fname) like "%doc%" or lower(fname) like "%ppt%" or lower(fname) like "%xls%" or lower(fname) like "%zip%" or lower(fname) like "%hwp%"') \
    # .where('lower(magic) like "%pdf%" or lower(magic) like "%microsoft%" or lower(magic) like "%zip%" or lower(magic) like "%execut%"') \.where('lower(magic) like "%pdf%" or lower(magic) like "%microsoft%" or lower(magic) like "%zip%" or lower(magic) like "%execut%"') \
    # .where('fname not like "%elastic%"') \
    # .where('fname not like "%api%" and fname not like "%wof%"') \
    # .where('lower(magic) not like "%text%" and lower(magic) not like "%gif%" and lower(magic) not like "%png%" and lower(magic) not like "%jpeg%"') \
    # .selectExpr("to_json(struct(*)) as value") \
    ##
# .where('lower(magic) like "%pdf%"') \


    # suricata_mod_log = suricata_log.withWatermark("timestamp", "10 minutes") \
    #     .selectExpr("cast (value as string) as json") \
    #     .select(from_json("json", schema).alias("data")) \
    #     .selectExpr("data.src_ip as src_ip", "data.event_type as event_typ", "data.dns.rrname as dns",
    #                 "data.dns.type as type", "data.timestamp as timestamps") \
    #     .where('type = "query"' and 'src_ip like "%192.168.2%"' and 'dns is not null') \
    #     .groupBy(window('timestamps', '60 minutes', '30 minutes'), 'dns') \
    #     .agg(count('dns'), max('timestamps'), max('src_ip')) \
    #     .orderBy("count(dns)", ascending=False)

    # suricata_mod_log.createOrReplaceTempView('tb')
    # suricata_mod_log = spark.sql("""
    #                     select t.*
    #                     from tb as t
    #                 """)\
        # .selectExpr("to_json(struct(*)) as value")

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

