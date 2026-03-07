import pytest
from src.business_logic import OrderProcessor
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("Testing").getOrCreate()

def test_filter_logic(spark):
    processor = OrderProcessor(spark)
    # Aqui criarias um pequeno DataFrame manual para testar se o filtro de 2025 
    # e status=false funciona.
    assert True # Placeholder para o seu teste