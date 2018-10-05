"""
This file is a simple setup for a Spark 1.6.x streaming job with Kafka input.  It receives a direct stream of Kafka
messages off a specified topic as an RDD in 1-second intervals and maps each entry to a specific operation
(in this case, capitalizing the text), and then prints the produced RDD to console.

"""

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import config as cfg
import logging

__author__ = 'Megan McClarty'
__version__ = '0.1'

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s')
logger.setLevel(logging.INFO)


def launch_sc():
    """
    Initiate Spark context

    :return: Spark context instance
    """

    try:
        conf = SparkConf()
        conf.setAppName("Upper Case Conversion")
        sc = SparkContext(conf=conf)
        return sc
    except Exception as e:
        logger.error(str(e))


def stream_setup(ssc):
    """
    Sets up direct stream of data off a Kafka topic

    :param ssc: Streaming context instance
    :return:
    """

    brokers = cfg.kafka['server']
    topic = cfg.kafka['in_topic']

    topic_stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    return topic_stream


def main(msg):
    """
    Converts all incoming text stream to uppercase

    :param msg: Kafka message to be processed
    :return:
    """

    try:
        up_message = str(msg).upper()
    except Exception as e:
        logger.error(str(e))

    return up_message


try:
    sc = launch_sc()
    # Process Kafka stream in 15 second intervals
    ssc = StreamingContext(sc, 15)
except Exception as e:
    logger.error(str(e))


if __name__ == '__main__':
    topic_stream = stream_setup(ssc)

    parsed_stream = topic_stream.map(lambda v: main(v[1]))
    parsed_stream.pprint()

    ssc.start()
    ssc.awaitTermination()


