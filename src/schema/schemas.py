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
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DoubleType(), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", TimestampType(), True),
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True)
    ]))
])