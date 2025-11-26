"""
Módulo de leitura e escrita de dados
"""

import logging
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, DateType, LongType, TimestampType
from pyspark.sql.utils import AnalysisException # Importar exceção específica do Spark

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataHandler:
    """
    Classe responsável pela leitura e escrita de dados
    """
    
    def __init__(self, spark_session):
        """
        Inicializa o handler de dados
        
        Args:
            spark_session: Instância da SparkSession
        """
        self.spark = spark_session
    
    def get_pagamentos_schema(self):
        """
        Define o schema explícito para o dataset de pagamentos
        
        Returns:
            StructType: Schema do dataset de pagamentos
        """
        avaliacao_fraude_schema = StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", FloatType(), True)
        ])
        
        return StructType([
            StructField("avaliacao_fraude", avaliacao_fraude_schema, True),
            StructField("data_processamento", StringType(), True),   # JSON é string ISO
            StructField("forma_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("valor_pagamento", FloatType(), True)
        ])

    
    def get_pedidos_schema(self):
        """
        Define o schema explícito para o dataset de pedidos
        
        Returns:
            StructType: Schema do dataset de pedidos
        """
        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", FloatType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True)
        ])
    
    def read_pagamentos(self, path):
        """
        Lê o dataset de pagamentos
        
        Args:
            path: Caminho para os arquivos de pagamentos
            
        Returns:
            DataFrame: DataFrame com os dados de pagamentos
        """
        try:
            schema = self.get_pagamentos_schema()
            df = self.spark.read \
                .schema(schema) \
                .option("compression", "gzip") \
                .json(path)
            logger.info(f"Leitura de pagamentos em {path} concluída com sucesso.")
            return df
        except AnalysisException as e:
            logger.error(f"Erro de análise ao ler pagamentos em {path}: {e}")
            # Retorna um DataFrame vazio em caso de erro de análise (caminho, formato, etc.)
            return self.spark.createDataFrame([], schema)
        except Exception as e:
            logger.error(f"Erro inesperado ao ler pagamentos em {path}: {e}")
            raise

    
    def read_pedidos(self, path):
        """
        Lê o dataset de pedidos
        
        Args:
            path: Caminho para os arquivos de pedidos
            
        Returns:
            DataFrame: DataFrame com os dados de pedidos
        """
        try:
            schema = self.get_pedidos_schema()
            df = self.spark.read \
                .schema(schema) \
                .option("header", "true") \
                .option("compression", "gzip") \
                .option("delimiter", ";") \
                .csv(path)
            logger.info(f"Leitura de pedidos em {path} concluída com sucesso.")
            return df
        except AnalysisException as e:
            logger.error(f"Erro de análise ao ler pedidos em {path}: {e}")
            # Retorna um DataFrame vazio em caso de erro de análise (caminho, formato, etc.)
            return self.spark.createDataFrame([], schema)
        except Exception as e:
            logger.error(f"Erro inesperado ao ler pedidos em {path}: {e}")
            raise
    
    def write_parquet(self, dataframe, path):
        """
        Escreve o DataFrame em formato Parquet
        
        Args:
            dataframe: DataFrame a ser escrito
            path: Caminho de destino
        """
        try:
            dataframe.write \
                .mode("overwrite") \
                .parquet(path)
            logger.info(f"Escrita de Parquet em {path} concluída com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao escrever Parquet em {path}: {e}")
            raise