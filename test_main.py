import unittest
import logging
import main

from pyspark.sql import SparkSession
from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.test_Transformer import test_Transformer

class TransformTest(unittest.TestCase):
    def test_method4(self):
        spark: SparkSession = SparkSession \
            .builder \
            .master(SPARK_MODE) \
            .getOrCreate()
        test_Transformer(spark, PARAMETER_EXERCISE_5)
        #This was made to recreate the funcionality of the principal function, also inside of this I recreated a similar
        #function to transform the data but with a small change, I counted after the regular flow and also i counted based
        #in my filters from method 4 and both quantities match so we can say it was a success. If both quantities wouldn't
        #match then there was enough evidence to say there was an error.
