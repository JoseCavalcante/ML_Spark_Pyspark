# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 07:59:37 2023

@author: Jose
"""

import findspark, pyspark

from pyspark.sql               import SparkSession
from pyspark.sql.functions     import isnan, when, count, col
from pyspark.ml                import Pipeline
from pyspark.ml.feature        import RFormula
from pyspark.ml.feature        import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation     import BinaryClassificationEvaluator
from pyspark.ml.evaluation     import MulticlassClassificationEvaluator
from pyspark.ml.feature        import StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.feature        import Binarizer
from pyspark.ml.feature        import Normalizer
from pyspark.ml.feature        import OneHotEncoder

findspark.init()
spark = SparkSession.builder.appName("pipeline").getOrCreate()

churnDF = spark.read.csv("Churn.csv", header=True, inferSchema=True, sep=";")
churnDF.show(5,truncate=False)