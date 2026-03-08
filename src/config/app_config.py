import os

class AppConfig:

    def __init__(self):

        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        self.pedidos_path = os.path.join(base_dir, "data/input/pedidos/*.csv.gz")
        self.pagamentos_path = os.path.join(base_dir, "data/input/pagamentos/*.json.gz")
        self.output_path = os.path.join(base_dir, "data/output/relatorio_recusados_2025")