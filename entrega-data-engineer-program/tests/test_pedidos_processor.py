# project_final/tests/test_pedidos_processor.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, DateType, LongType
from datetime import date
from src.config.app_config import AppConfig
from src.business.pedidos_processor import PedidosProcessor

# --- Fixtures ---

@pytest.fixture(scope="module")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    """
    spark = SparkSession.builder \
        .appName("TestPedidosProcessor") \
        .master("local[1]") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()

@pytest.fixture
def app_config():
    """
    Fixture para criar configurações de teste
    """
    # Usamos o AppConfig real, mas sobrescrevemos os filtros para o teste
    config = AppConfig()
    config.ano_filtro = 2025 # Garante que o filtro de ano esteja correto
    config.status_pagamento_filtro = False # Pagamentos recusados
    config.fraude_filtro = False # Fraude legítima
    return config

@pytest.fixture
def pedidos_processor(app_config, spark_session):
    """
    Fixture para criar instância do processador
    """
    return PedidosProcessor(app_config, spark_session)

# --- Schemas de Teste ---

def get_pagamentos_schema_teste():
    # Schema de pagamentos (usando o padrão MAIÚSCULAS do DataFrame final)
    avaliacao_fraude_schema = StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", FloatType(), True)
    ])
    
    return StructType([
        StructField("ID_PEDIDO", StringType(), True),
        StructField("FORMA_PAGAMENTO", StringType(), True),
        StructField("VALOR_PAGAMENTO", FloatType(), True),
        StructField("STATUS", BooleanType(), True),
        StructField("DATA_PROCESSAMENTO", DateType(), True),
        StructField("AVALIACAO_FRAUDE", avaliacao_fraude_schema, True)
    ])

def get_pedidos_schema_teste():
    # Schema de pedidos
    return StructType([
        StructField("ID_PEDIDO", StringType(), True),
        StructField("PRODUTO", StringType(), True),
        StructField("VALOR_UNITARIO", FloatType(), True),
        StructField("QUANTIDADE", LongType(), True),
        StructField("DATA_CRIACAO", DateType(), True),
        StructField("UF", StringType(), True),
        StructField("ID_CLIENTE", LongType(), True)
    ])

# --- Teste Principal ---

def test_processar_relatorio_completo(spark_session, pedidos_processor):
    """
    Testa o pipeline completo de processamento (processar_relatorio)
    para garantir que a lógica de negócio esteja correta.
    """
    
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    
    # Dados de Pagamentos:
    # - pedido1: STATUS=False, FRAUDE=False (INCLUÍDO)
    # - pedido2: STATUS=True, FRAUDE=False (EXCLUÍDO - status não é False)
    # - pedido3: STATUS=False, FRAUDE=True (EXCLUÍDO - fraude não é False)
    # - pedido4: STATUS=False, FRAUDE=False (EXCLUÍDO pelo filtro de ANO (2024))
    pagamentos_data = [
        ("pedido1", "Pix", 100.0, False, date(2025, 1, 1), {"fraude": False, "score": 0.1}),
        ("pedido2", "Boleto", 200.0, True, date(2025, 1, 2), {"fraude": False, "score": 0.2}),
        ("pedido3", "Cartão", 300.0, False, date(2025, 1, 3), {"fraude": True, "score": 0.9}),
        ("pedido4", "Pix", 400.0, False, date(2024, 12, 31), {"fraude": False, "score": 0.15})
    ]
    df_pagamentos = spark_session.createDataFrame(pagamentos_data, get_pagamentos_schema_teste())
    
    # Dados de Pedidos:
    # - pedido1: (10*2) + (15*3) = 65.0, DATA_CRIACAO=2025 (INCLUÍDO)
    # - pedido4: (20*1) = 20.0, DATA_CRIACAO=2024 (EXCLUÍDO pelo filtro de ANO)
    pedidos_data = [
        ("pedido1", "Produto A", 10.0, 2, date(2025, 1, 1), "SP", 1),
        ("pedido1", "Produto B", 15.0, 3, date(2025, 1, 1), "SP", 1),
        ("pedido4", "Produto C", 20.0, 1, date(2024, 12, 31), "RJ", 2)
    ]
    df_pedidos = spark_session.createDataFrame(pedidos_data, get_pedidos_schema_teste())
    
    # 2. Act (Executar a função a ser testada)
    df_relatorio = pedidos_processor.processar_relatorio(df_pagamentos, df_pedidos)
    
    # 3. Assert (Verificar se o resultado é o esperado)
    
    # O único pedido que atende a TODOS os critérios é o 'pedido1' (Status=False, Fraude=False, Ano=2025)
    assert df_relatorio.count() == 1, "Deve retornar apenas 1 registro que atende a todos os critérios"
    
    resultado = df_relatorio.first()
    
    # Verificar as colunas do relatório final (id_pedido, estado, forma_pagamento, valor_total, data_pedido)
    assert resultado["id_pedido"] == "pedido1", "Deve retornar o pedido1"
    assert resultado["estado"] == "SP", "Estado deve ser SP"
    assert resultado["forma_pagamento"] == "Pix", "Forma de pagamento deve ser Pix"
    assert resultado["valor_total"] == 65.0, "Valor total deve ser 65.0"
    assert resultado["data_pedido"] == date(2025, 1, 1), "Data do pedido deve ser 2025-01-01"