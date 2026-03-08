class DataWriter:

    def write_parquet(self, df, path):

        df.write \
            .mode("overwrite") \
            .csv(path)