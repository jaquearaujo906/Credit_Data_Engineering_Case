# SIAPE Lakehouse – Desafio de Engenheiro de Dados

Este projeto implementa uma arquitetura Lakehouse completa usando Databricks e Delta Lake para processar dados públicos do SIAPE do Portal da Transparência Brasileira. O objetivo é construir um produto de dados robusto, pronto para produção, capaz de servir os times de Modelagem de Crédito e Política de Crédito, garantindo consistência histórica (M-2 + 3 meses anteriores), linhagem de dados, escalabilidade e usabilidade analítica.


A arquitetura segue este fluxo lógico: dados são ingeridos de arquivos CSV baixados do Portal da Transparência; os arquivos são organizados por posição mensal (posicao=YYYY-MM) e processados incrementalmente. A camada Bronze armazena dados brutos ingeridos em formato Delta, preservando metadados e particionamento por posicao. A camada Silver aplica lógica de normalização e limpeza, incluindo padronização de CPF, conversão de valores monetários, normalização de colunas e padronização de domínios. A camada Gold consolida todos os domínios em um único conjunto de dados pronto para análise com granularidade de Servidor × Mês.

Visão Geral da Arquitetura:

Portal da Transparência (arquivos CSV SIAPE)
        ↓
Camada Bronze (Delta bruto, particionado por posicao, com metadados)
        ↓
Camada Silver (normalização de CPF, conversão de tipos de dados, limpeza, padronização)
        ↓
Camada Gold (conjunto de dados consolidado servidor-mês)


Lógica de Janela Histórica:
O conjunto de dados processa automaticamente a posição M-2 mais recente e os três meses anteriores (M-3, M-4, M-5). Todas as tabelas são particionadas por posicao (YYYY-MM), permitindo ingestão incremental e evitando reprocessamento histórico completo.

Camada Bronze:
Propósito: ingestão bruta e rastreabilidade.
- Ingestão de CSV com tratamento adequado de codificação e delimitadores
- Enriquecimento de metadados (posicao, timestamp de ingestão, domínio de origem)
- Particionado por posicao
- Sem transformação de negócio
Tabelas criadas:
workspace.public_informations.bronze_siape_remuneracao
workspace.public_informations.bronze_siape_cadastro
workspace.public_informations.bronze_siape_afastamento
workspace.public_informations.bronze_siape_observacoes

Camada Silver:
Propósito: padronização de dados e consistência estrutural.
- Normalização de CPF (remover formatação e padronizar para 11 dígitos)
- Conversão de campos monetários (string para numérico)
- Renomeação e limpeza de colunas
- Ajustes de consistência de domínio
Tabelas criadas:
workspace.public_informations.silver_siape_remuneracao
workspace.public_informations.silver_siape_cadastro
workspace.public_informations.silver_siape_afastamento
workspace.public_informations.silver_siape_observacoes

Camada Gold (Produto Final):
A tabela consolidada final é:

workspace.public_informations.gold_siape_servidor_mensal

A camada Gold une remuneração, cadastro, afastamento (agregado) e observações (agregado) usando cpf_norm e posicao como chaves primárias. O conjunto de dados final inclui CPF normalizado, métricas de remuneração, grupo de ocupação, indicador de afastamento, número de observações e colunas de metadados.

Estatísticas Descritivas Implementadas:
A solução inclui análise estatística descritiva sobre o conjunto de dados Gold final, incluindo:
- Número total de registros
- Contagem de CPFs únicos
- Análise de distribuição de valores nulos para colunas-chave
- Distribuição por grupo_ocupacao
- Validação estrutural de junções
Essas estatísticas validam completude de dados, usabilidade de negócio, integridade estrutural e consistência histórica.

Ordem de Execução:
1. 00_siape_config_and_utils
2. 01_siape_bronze_ingestion
3. 02_siape_silver_standardization
4. 03_siape_gold_product_and_stats

Considerações de Prontidão para Produção:
A solução suporta ingestão incremental mensal, preserva partições históricas, mantém linhagem de dados, habilita processamento Delta escalável e separa lógica de ingestão de lógica de transformação. A arquitetura é projetada para manutenibilidade, evolução e extensão para conjuntos de dados de informações públicas adicionais dentro do mesmo esquema.

Fonte de Dados:
Portal da Transparência Brasileira
https://portaldatransparencia.gov.br/download-de-dados/servidores

Tecnologias Utilizadas:
- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Padrão de Arquitetura Lakehouse

Entregáveis:
1. Tabela de produto Gold: workspace.public_informations.gold_siape_servidor_mensal
2. Código-fonte completo implementando ingestão Bronze, padronização Silver, consolidação Gold 
3. Código-fonte completo implementando estatísticas descritivas.