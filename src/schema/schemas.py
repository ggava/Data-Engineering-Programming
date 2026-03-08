from pyspark.sql.types import *

pedidos_schema = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", DoubleType(), True),
    StructField("QUANTIDADE", IntegerType(), True),
    StructField("DATA_CRIACAO", TimestampType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", StringType(), True)
])

pagamentos_schema = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("FORMA_PAGAMENTO", StringType(), True),
    StructField("VALOR_PAGAMENTO", DoubleType(), True),
    StructField("STATUS", BooleanType(), True),
    StructField("DATA_PROCESSAMENTO", TimestampType(), True),
    StructField("AVALIACAO_FRAUDE", StructType([
        StructField("fraude", BooleanType(), True)
    ]))
])