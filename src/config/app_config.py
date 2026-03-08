import os

class AppConfig:
    def __init__(self):
        # Usamos o caminho absoluto direto do seu ambiente Cloud9 para evitar erros de lógica
        base_dir = "/home/ubuntu/environment/projeto_pyspark"
        
        print(f"DEBUG - Caminho Base: {base_dir}")

        # Agora montamos os caminhos a partir da raiz correta
        self.pedidos_path = os.path.join(base_dir, "data", "input", "pedidos", "*.csv.gz")
        self.pagamentos_path = os.path.join(base_dir, "data", "input", "pagamentos", "*.json.gz")
        self.output_path = os.path.join(base_dir, "data", "output", "relatorio_recusados_2025")
        
        print(f"DEBUG - Procurando Pedidos em: {self.pedidos_path}")
        print(f"DEBUG - Procurando Pagamentos em: {self.pagamentos_path}")