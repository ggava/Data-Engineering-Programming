import logging

from config.app_config import AppConfig
from spark.spark_manager import SparkManager
from io_utils.data_reader import DataReader
from io_utils.data_writer import DataWriter
from business_logic import OrderProcessor
from pipeline.pipeline_orchestrator import PipelineOrchestrator
from schema.schemas import pedidos_schema, pagamentos_schema


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():

    # =================================
    # Instancia configuração
    # =================================

    config = AppConfig()

    # =================================
    # Cria sessão Spark
    # =================================

    spark_manager = SparkManager()
    spark = spark_manager.create_session()
    
    # =================================
    # Configurando schemas
    # =================================
    schemas = {
        "pedidos": pedidos_schema,
        "pagamentos": pagamentos_schema
    }
    
    # =================================
    # Instancia dependências
    # =================================

    reader = DataReader(spark)
    writer = DataWriter()
    processor = OrderProcessor()

    # =================================
    # Orquestra pipeline
    # =================================

    pipeline = PipelineOrchestrator(
        reader,
        writer,
        processor,
        config,
        schemas
    )
    
    # Executa pipeline
    pipeline.run()

    spark.stop()


if __name__ == "__main__":
    main()