import sys, os
from pyspark import SparkContext, SparkConf

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import cloudpickle
import json
import kafka


producer = kafka.KafkaProducer(bootstrap_servers='192.168.2.12:9096', value_serializer=lambda m: json.dumps(m))
# , value_serializer=lambda m: json.dumps(m).encode('utf-8')


global res


def updateTotalCnt(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)


def hello(msg):
    ii = []
    res = msg.collect()
    x = sorted(res, key=lambda k: k[1], reverse=True)[:50]
    # xx = dict(x)
    # yy = sorted(xx.items(), key=itemgetter(1), reverse=True)
    try:
        for i in x:
            x = {}
            x['dns'] = str(i[0]).strip().encode('utf-8')
            x['cnt'] = int(i[1])
            ii.append(x)

        print(ii)
        producer.send(topic='test1test', value=ii)
        producer.flush()
        pass
    except Exception as e:
        print("hello world")
        print(res)
        print(e)

if __name__ == "__main__":

    print("1. start")

    conf = SparkConf()
    conf.set("spark.local.ip", "192.168.2.12")
    conf.set("spark.driver.host", "192.168.2.12")
    # conf.set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.1.0")
    # conf.set("spark.jars", ",".join(exampleJars))

    print("2. spark context")

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount11111")
    sc.setCheckpointDir('/home/tmp/check')
    ssc = StreamingContext(sc, 5)

    print("3. kafka stream")

    # ds1 = KafkaUtils.createStream(ssc, "192.168.16.250:22181", "test-consumer-group", {"syslog.cef.original": 5})
    ds1 = KafkaUtils.createStream(ssc, "192.168.2.12:2181", "test-consumer-group", {"suricata12": 1})

    print("4. count word")

    print("5. print counts result")

    lines = ds1.map(lambda x: json.loads(x[1]))
    # dns = lines.map(lambda x: json.loads(x['message']))

    dns_cnt = lines.filter(lambda x: x['event_type'] == 'dns')
    dns_cnt.pprint()
    # dns_type = dns_cnt.filter(lambda x: x['dns']['type'] == 'query')
    # dns_rrname = dns_type.filter(lambda x: x['dns']['rrname'] is not None)
    # dns_rrname = dns_rrname.filter(lambda x: x['dns']['rrname'] is not u'')
    # dns_rank = dns_rrname.map(lambda x: (x['dns']['rrname'], 1)).reduceByKey(lambda a, b: a+b)
    # dns_tot = dns_rank.updateStateByKey(updateTotalCnt)
    # dns_tot.foreachRDD(hello)

    print("6. spark streaming context start!")

    ssc.start()
    ssc.awaitTermination()

