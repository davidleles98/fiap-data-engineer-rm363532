"""
Módulo de configuração da aplicação
"""

class AppConfig:
    """
    Classe responsável por centralizar as configurações da aplicação
    """
    
    def __init__(self):
        """
        Inicializa as configurações da aplicação
        """
        # Configurações do Spark
        self.app_name = "PedidosRecusadosLegitimos"
        self.master = "local[*]"
        
        # Configurações de caminhos
        self.pagamentos_path = "data/input/pagamentos/pagamentos.gz"
        self.pedidos_path = "data/input/pedidos/pedidos.gz"
        self.output_path = "data/output/relatorio_pedidos"
        
        # Configurações de negócio
        self.ano_filtro = 2025
        self.status_pagamento_filtro = False
        self.fraude_filtro = False
        
        # Configurações de formato
        self.formato_output = "parquet"
        
    def get_spark_config(self):
        """
        Retorna as configurações do Spark
        
        Returns:
            dict: Dicionário com as configurações do Spark
        """
        return {
            "app_name": self.app_name,
            "master": self.master
        }
    
    def get_paths_config(self):
        """
        Retorna as configurações de caminhos
        
        Returns:
            dict: Dicionário com os caminhos
        """
        return {
            "pagamentos_path": self.pagamentos_path,
            "pedidos_path": self.pedidos_path,
            "output_path": self.output_path
        }
    
    def get_business_config(self):
        """
        Retorna as configurações de negócio
        
        Returns:
            dict: Dicionário com as regras de negócio
        """
        return {
            "ano_filtro": self.ano_filtro,
            "status_pagamento_filtro": self.status_pagamento_filtro,
            "fraude_filtro": self.fraude_filtro
        }