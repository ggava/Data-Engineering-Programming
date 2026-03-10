import pytest
from src.business_logic import OrderProcessor
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from datetime import date
 
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("Testing").getOrCreate()
 
@pytest.fixture
def order_processor():
    return OrderProcessor()
 
def test_filter_only_2025_records(spark, order_processor):
    """
    Garante que apenas registros do ano de 2025 sejam mantidos.
    """
    # 1. Mock de Pedidos (Simulando colunas originais que sua classe renomeia)
    data_pedidos = [
        (1, 100.0, 1, date(2024, 12, 31), "SP"), # Ano errado
        (2, 200.0, 1, date(2025, 1, 15), "RJ"),  # CORRETO
        (3, 300.0, 1, date(2026, 6, 1), "MG")    # Ano errado
    ]
    schema_pedidos = StructType([
        StructField("ID_PEDIDO", IntegerType(), True),
        StructField("VALOR_UNITARIO", DoubleType(), True),
        StructField("QUANTIDADE", IntegerType(), True),
        StructField("DATA_CRIACAO", DateType(), True),
        StructField("UF", StringType(), True)
    ])
 
    # 2. Mock de Pagamentos (Simulando estrutura complexa AVALIACAO_FRAUDE.fraude)
    data_pagamentos = [
        (1, "Cartao", False, {"fraude": False}), 
        (2, "Boleto", False, {"fraude": False}), 
        (3, "Pix", False, {"fraude": False})     
    ]
    schema_pagamentos = StructType([
        StructField("ID_PEDIDO", IntegerType(), True),
        StructField("FORMA_PAGAMENTO", StringType(), True),
        StructField("STATUS", BooleanType(), True),
        StructField("AVALIACAO_FRAUDE", StructType([
            StructField("fraude", BooleanType(), True)
        ]), True)
    ])
 
    df_ped_mock = spark.createDataFrame(data_pedidos, schema_pedidos)
    df_pag_mock = spark.createDataFrame(data_pagamentos, schema_pagamentos)
 
    # CHAMADA CORRETA: Passando os DOIS argumentos exigidos
    result_df = order_processor.process_rejected_orders(df_ped_mock, df_pag_mock)
 
    # Validações
    assert result_df.count() == 1
    assert result_df.collect()[0]["id_pedido"] == 2
    assert result_df.collect()[0]["valor_total"] == 200.0
 
 
def test_output_schema_integrity(spark, order_processor):
    """
    Valida se o relatório final possui apenas as colunas exigidas.
    """
    data_ped = [(10, 50.0, 2, date(2025, 5, 20), "SP")]
    schema_ped = StructType([
        StructField("ID_PEDIDO", IntegerType(), True),
        StructField("VALOR_UNITARIO", DoubleType(), True),
        StructField("QUANTIDADE", IntegerType(), True),
        StructField("DATA_CRIACAO", DateType(), True),
        StructField("UF", StringType(), True)
    ])
 
    data_pag = [(10, "Pix", False, {"fraude": False})]
    schema_pag = StructType([
        StructField("ID_PEDIDO", IntegerType(), True),
        StructField("FORMA_PAGAMENTO", StringType(), True),
        StructField("STATUS", BooleanType(), True),
        StructField("AVALIACAO_FRAUDE", StructType([
            StructField("fraude", BooleanType(), True)
        ]), True)
    ])
 
    df_ped = spark.createDataFrame(data_ped, schema_ped)
    df_pag = spark.createDataFrame(data_pag, schema_pag)
 
    result_df = order_processor.process_rejected_orders(df_ped, df_pag)
 
    # Colunas finais esperadas (minúsculas após seu processamento)
    expected_columns = ["id_pedido", "uf", "forma_pagamento", "valor_total", "data_pedido"]
    for col in expected_columns:
        assert col in result_df.columns
    # Garante que colunas de filtro não "vazaram" para o relatório final
    assert "status_pagamento" not in result_df.columns