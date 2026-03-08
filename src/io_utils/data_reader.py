from pyspark.sql import functions as F

class DataReader:
    def __init__(self, spark):
        self.spark = spark

    def read_pedidos(self, path, schema):
        # Removemos o asterisco do path se ele existir e lemos a pasta
        return (
            self.spark.read
            .schema(schema)
            .option("header", True)
            .option("sep", ";")
            .csv(path)
        )

    def read_pagamentos(self, path, schema):
        # Lendo a pasta diretamente
        return (
            self.spark.read
            .schema(schema)
            .json(path)
        )