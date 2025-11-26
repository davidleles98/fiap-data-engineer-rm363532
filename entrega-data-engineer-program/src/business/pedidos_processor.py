"""
Módulo de lógica de negócios para processamento de pedidos
"""

import logging
from pyspark.sql import SparkSession

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PedidosProcessor:
    """
    Classe responsável pela lógica de negócios de processamento de pedidos
    Utiliza Spark SQL para processamento de dados
    """
    
    def __init__(self, app_config, spark_session):
        """
        Inicializa o processador de pedidos
        
        Args:
            app_config: Instância de AppConfig com as configurações
            spark_session: Instância da SparkSession para executar SQL
        """
        self.app_config = app_config
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
    
    def processar_relatorio(self, df_pagamentos, df_pedidos):
        """
        Processa o relatório conforme requisitos de negócio usando Spark SQL
        
        Args:
            df_pagamentos: DataFrame de pagamentos
            df_pedidos: DataFrame de pedidos
            
        Returns:
            DataFrame: DataFrame processado com o relatório final
        """
        try:
            self.logger.info("Iniciando processamento do relatório com Spark SQL")
            
            # Registrar DataFrames como views temporárias
            self.logger.info("Registrando tabelas temporárias")
            df_pagamentos.createOrReplaceTempView("pagamentos")
            df_pedidos.createOrReplaceTempView("pedidos")
            
            # Etapa 1: Filtrar pagamentos recusados e legítimos
            self.logger.info("Filtrando pagamentos recusados e legítimos")
            df_pagamentos_filtrado = self._filtrar_pagamentos()
            df_pagamentos_filtrado.createOrReplaceTempView("pagamentos_filtrados")
            
            # Etapa 2: Calcular valor total do pedido
            self.logger.info("Calculando valor total dos pedidos")
            df_pedidos_agregado = self._agregar_pedidos()
            df_pedidos_agregado.createOrReplaceTempView("pedidos_agregados")
            
            # Etapa 3: Realizar join e filtrar por ano
            self.logger.info(f"Realizando join e filtrando por ano {self.app_config.ano_filtro}")
            df_relatorio = self._gerar_relatorio()
            
            self.logger.info("Processamento do relatório concluído com sucesso")
            return df_relatorio
            
        except Exception as e:
            self.logger.error(f"Erro no processamento do relatório: {str(e)}")
            raise
    
    def _filtrar_pagamentos(self):
        """
        Filtra pagamentos recusados (status=false) e legítimos (fraude=false) usando SQL
        
        Returns:
            DataFrame: DataFrame filtrado
        """
        query = f"""
            SELECT 
                ID_PEDIDO,
                FORMA_PAGAMENTO,
                VALOR_PAGAMENTO,
                STATUS,
                DATA_PROCESSAMENTO,
                AVALIACAO_FRAUDE
            FROM pagamentos
            WHERE STATUS = {str(self.app_config.status_pagamento_filtro).upper()}
              AND AVALIACAO_FRAUDE.fraude = {str(self.app_config.fraude_filtro).upper()}
        """
        
        return self.spark.sql(query)
    
    def _agregar_pedidos(self):
        """
        Agrega pedidos calculando o valor total usando SQL
        
        Returns:
            DataFrame: DataFrame agregado
        """
        query = """
            SELECT 
                ID_PEDIDO,
                UF,
                DATA_CRIACAO,
                SUM(VALOR_UNITARIO * QUANTIDADE) AS VALOR_TOTAL
            FROM pedidos
            GROUP BY ID_PEDIDO, UF, DATA_CRIACAO
        """
        
        return self.spark.sql(query)
    
    def _gerar_relatorio(self):
        """
        Gera o relatório final com join, filtro por ano, seleção de colunas e ordenação
        
        Returns:
            DataFrame: DataFrame com o relatório final
        """
        query = f"""
            SELECT 
                p.ID_PEDIDO AS id_pedido,
                pa.UF AS estado,
                p.FORMA_PAGAMENTO AS forma_pagamento,
                pa.VALOR_TOTAL AS valor_total,
                pa.DATA_CRIACAO AS data_pedido
            FROM pagamentos_filtrados p
            INNER JOIN pedidos_agregados pa
                ON p.ID_PEDIDO = pa.ID_PEDIDO
            WHERE YEAR(pa.DATA_CRIACAO) = {self.app_config.ano_filtro}
            ORDER BY estado, forma_pagamento, data_pedido
        """
        
        return self.spark.sql(query)