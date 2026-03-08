from pyspark.sql import functions as F

class DataReader:

    def __init__(self, spark):
        self.spark = spark

    def read_pedidos(self, path, schema):

        df = (
            self.spark.read
            .schema(schema)
            .option("header", True)
            .option("sep", ";")
            .csv(path)
        )

        return df

    def read_pagamentos(self, path, schema):

        df = (
            self.spark.read
            .schema(schema)
            .json(path)
        )

        return df