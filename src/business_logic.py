import logging
from pyspark.sql import functions as F


class OrderProcessor:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_rejected_orders(self, df_pedidos, df_pagamentos):

        try:
            print("Pagamentos:", df_pagamentos.count())
            print("Pedidos:", df_pedidos.count())

            # -------------------------
            # Normalização pagamentos
            # -------------------------
            df_pagamentos = (
                df_pagamentos
                .withColumnRenamed("ID_PEDIDO", "id_pedido")
                .withColumnRenamed("FORMA_PAGAMENTO", "forma_pagamento")
                .withColumnRenamed("VALOR_PAGAMENTO", "valor_pagamento")
                .withColumnRenamed("STATUS", "status_pagamento")
                .withColumnRenamed("DATA_PROCESSAMENTO", "data_processamento")
                .withColumn("fraude", F.col("AVALIACAO_FRAUDE.fraude"))
            )

            # -------------------------
            # Normalização pedidos
            # -------------------------
            df_pedidos = (
                df_pedidos
                .withColumnRenamed("ID_PEDIDO", "id_pedido")
                .withColumnRenamed("PRODUTO", "produto")
                .withColumnRenamed("VALOR_UNITARIO", "valor_unitario")
                .withColumnRenamed("QUANTIDADE", "quantidade")
                .withColumnRenamed("DATA_CRIACAO", "data_pedido")
                .withColumnRenamed("UF", "uf")
                .withColumnRenamed("ID_CLIENTE", "id_cliente")
            )

            

            self.logger.info("Iniciando join entre pedidos e pagamentos...")

            # -------------------------
            # Join
            # -------------------------
            df_join = df_pedidos.join(df_pagamentos, "id_pedido")

            print("Join:", df_join.count())

            # -------------------------
            # Filtro
            # -------------------------
            df_filtrado = df_join.filter(
                (F.col("status_pagamento") == False) &
                (F.col("fraude") == False) &
                (F.year(F.col("data_pedido")) == 2025)
            )

            # -------------------------
            # Seleção final
            # -------------------------
            df_final = (
                df_filtrado
                .select(
                    "id_pedido",
                    "uf",
                    "forma_pagamento",
                    (F.col("valor_unitario") * F.col("quantidade")).alias("valor_total"),
                    "data_pedido"
                )
                .orderBy(
                    "uf",
                    "forma_pagamento",
                    "data_pedido"
                )
            )
            
            df_final.show(20, truncate=False)
            
            return df_final
            
            

        except Exception as e:
            self.logger.error(f"Erro ao processar dados: {e}")
            raise