"""
Módulo principal da aplicação - Aggregation Root
"""

import sys
from src.config.app_config import AppConfig
from src.session.spark_session_manager import SparkSessionManager
from src.io.data_handler import DataHandler
from src.business.pedidos_processor import PedidosProcessor
from src.orchestration.pipeline_orchestrator import PipelineOrchestrator

def main():
    """
    Função principal - Aggregation Root
    Responsável por instanciar e injetar todas as dependências
    """
    
    # Instanciar configurações
    app_config = AppConfig()
    
    # Instanciar gerenciador de sessão Spark
    spark_manager = SparkSessionManager(app_config)
    spark_session = spark_manager.get_session()
    
    # Instanciar handler de dados (injetando spark_session)
    data_handler = DataHandler(spark_session)
    
    # Instanciar processador de pedidos (injetando app_config e spark_session para SQL)
    pedidos_processor = PedidosProcessor(app_config, spark_session)
    
    # Instanciar orquestrador do pipeline (injetando todas as dependências)
    pipeline_orchestrator = PipelineOrchestrator(
        data_handler=data_handler,
        pedidos_processor=pedidos_processor,
        app_config=app_config
    )
    
    # Executar pipeline
    sucesso = pipeline_orchestrator.executar_pipeline()
    
    # Finalizar sessão Spark
    spark_manager.stop_session()
    
    # Retornar código de saída
    return 0 if sucesso else 1

if __name__ == "__main__":
    sys.exit(main())