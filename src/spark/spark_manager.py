from pyspark.sql import SparkSession


class SparkManager:

    def create_session(self):

        return (
            SparkSession.builder
            .appName("RelatorioPedidos2025")
            .getOrCreate()
        )