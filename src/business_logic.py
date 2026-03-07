import logging
from pyspark.sql import functions as F

class OrderProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def process_rejected_orders(self, df_pedidos, df_pagamentos):
        try:
            self.logger.info("A iniciar o join entre pedidos e pagamentos...")
            
            # Join dos datasets pelo ID do pedido
            df_join = df_pedidos.join(df_pagamentos, "id_pedido")

            # Filtros: 
            # 1. Pagamento recusado (status_pagamento == false)
            # 2. Não é fraude (fraude == false)
            # 3. Ano de 2025
            df_filtrado = df_join.filter(
                (F.col("status_pagamento") == False) & 
                (F.col("fraude") == False) &
                (F.year(F.col("data_pedido")) == 2025)
            )

            # Seleção e Ordenação conforme o PDF
            df_final = df_filtrado.select(
                "id_pedido", 
                "uf", 
                "forma_pagamento", 
                "valor_total", 
                "data_pedido"
            ).orderBy("uf", "forma_pagamento", "data_pedido")

            return df_final
        except Exception as e:
            self.logger.error(f"Erro ao processar dados: {e}")
            raise