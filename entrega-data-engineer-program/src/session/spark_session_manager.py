"""
Módulo de gerenciamento de sessão Spark
"""

from pyspark.sql import SparkSession

class SparkSessionManager:
    """
    Classe responsável por gerenciar a sessão Spark
    """
    
    def __init__(self, app_config):
        """
        Inicializa o gerenciador de sessão Spark
        
        Args:
            app_config: Instância de AppConfig com as configurações
        """
        self.app_config = app_config
        self._spark_session = None
    
    def get_session(self):
        """
        Retorna ou cria a sessão Spark
        
        Returns:
            SparkSession: Instância da sessão Spark
        """
        if self._spark_session is None:
            spark_config = self.app_config.get_spark_config()
            self._spark_session = SparkSession.builder \
                .appName(spark_config["app_name"]) \
                .master(spark_config["master"]) \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .getOrCreate()
        
        return self._spark_session
    
    def stop_session(self):
        """
        Finaliza a sessão Spark
        """
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None