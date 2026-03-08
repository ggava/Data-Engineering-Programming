import os

class AppConfig:
    def __init__(self):
        # Deteta a pasta 'projeto_pyspark' baseada na localização deste ficheiro
        # caminho: projeto_pyspark/src/config/app_config.py
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
        
        # Monta os caminhos sem repetir 'projeto_pyspark'
        self.pedidos_path = os.path.join(self.base_dir, "data", "input", "pedidos")
        self.pagamentos_path = os.path.join(self.base_dir, "data", "input", "pagamentos")
        self.output_path = os.path.join(self.base_dir, "data", "output", "relatorio_recusados_2025")