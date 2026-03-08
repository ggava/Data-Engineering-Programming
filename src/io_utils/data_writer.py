class DataWriter:

    def write_parquet(self, df, path):

        df.write \
            .mode("overwrite") \
            .parquet(path)