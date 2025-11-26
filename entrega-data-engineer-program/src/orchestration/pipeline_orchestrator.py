"""
Módulo de orquestração do pipeline
"""

import logging

class PipelineOrchestrator:
    """
    Classe responsável pela orquestração do pipeline de processamento
    """
    
    def __init__(self, data_handler, pedidos_processor, app_config):
        """
        Inicializa o orquestrador do pipeline
        
        Args:
            data_handler: Instância de DataHandler
            pedidos_processor: Instância de PedidosProcessor
            app_config: Instância de AppConfig
        """
        self.data_handler = data_handler
        self.pedidos_processor = pedidos_processor
        self.app_config = app_config
        self.logger = logging.getLogger(__name__)
    
    def executar_pipeline(self):
        """
        Executa o pipeline completo de processamento
        
        Returns:
            bool: True se executado com sucesso, False caso contrário
        """
        try:
            self.logger.info("="*60)
            self.logger.info("INICIANDO PIPELINE DE PROCESSAMENTO")
            self.logger.info("="*60)
            
            # Obter caminhos das configurações
            paths_config = self.app_config.get_paths_config()
            
            # Etapa 1: Leitura dos dados
            self.logger.info("Etapa 1: Leitura dos dados")
            df_pagamentos = self.data_handler.read_pagamentos(
                paths_config["pagamentos_path"]
            )
            self.logger.info(f"Total de pagamentos lidos: {df_pagamentos.count()}")
            
            df_pedidos = self.data_handler.read_pedidos(
                paths_config["pedidos_path"]
            )
            self.logger.info(f"Total de pedidos lidos: {df_pedidos.count()}")
            
            # Etapa 2: Processamento do relatório
            self.logger.info("Etapa 2: Processamento do relatório")
            df_relatorio = self.pedidos_processor.processar_relatorio(
                df_pagamentos,
                df_pedidos
            )
            self.logger.info(f"Total de registros no relatório: {df_relatorio.count()}")
            
            # Etapa 3: Escrita do resultado
            self.logger.info("Etapa 3: Escrita do resultado")
            self.data_handler.write_parquet(
                df_relatorio,
                paths_config["output_path"]
            )
            self.logger.info(f"Relatório salvo em: {paths_config['output_path']}")
            
            # Etapa 4: Exibir amostra do resultado
            self.logger.info("Etapa 4: Amostra do resultado (primeiras 20 linhas)")
            df_relatorio.show(20, truncate=False)
            
            self.logger.info("="*60)
            self.logger.info("PIPELINE EXECUTADO COM SUCESSO")
            self.logger.info("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro na execução do pipeline: {str(e)}")
            return False