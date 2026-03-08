class PipelineOrchestrator:

    def __init__(self, reader, writer, processor, config, schemas):

        self.reader = reader
        self.writer = writer
        self.processor = processor
        self.config = config
        self.schemas = schemas

    def run(self):

        df_pedidos = self.reader.read_pedidos(
            self.config.pedidos_path,
            self.schemas["pedidos"]
        )

        df_pagamentos = self.reader.read_pagamentos(
            self.config.pagamentos_path,
            self.schemas["pagamentos"]
        )

        relatorio = self.processor.process_rejected_orders(
            df_pedidos,
            df_pagamentos
        )

        self.writer.write_parquet(
            relatorio,
            self.config.output_path
        )