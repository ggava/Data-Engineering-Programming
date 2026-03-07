import logging
from pyspark.sql import SparkSession
from business_logic import OrderProcessor

# Configuração de Logging conforme pedido no item 9 do PDF
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    spark = SparkSession.builder.appName("RelatorioPedidos2025").getOrCreate()
    processor = OrderProcessor(spark)

    # Lembre-se de ajustar os caminhos conforme a sua pasta data/
    df_pedidos = spark.read.csv("./projeto_pyspark/data/input/pedidos/*.csv.gz", header=True, inferSchema=True)
    df_pedidos.withColumnRenamed("ID_PEDIDO", "id_pedido")
    print(df_pedidos.columns)
    
    df_pagamentos = spark.read.json("./projeto_pyspark/data/input/pagamentos/*.json.gz")
    print(df_pagamentos.columns)
    

    # Executa a lógica
    relatorio = processor.process_rejected_orders(df_pedidos, df_pagamentos)

    # Gravação em Parquet (Requisito do item 4 do Escopo)
    relatorio.write.mode("overwrite").csv("./projeto_pyspark/data/output/relatorio_recusados_2025.csv")
    
    logging.info("Trabalho concluído com sucesso e salvo em CSV.")

if __name__ == "__main__":
    main()