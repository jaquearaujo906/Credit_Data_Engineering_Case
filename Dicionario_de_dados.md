## Dicionário de Dados – gold_siape_servidor_mensal

Tabela: `public_informations.gold_siape_servidor_mensal`  
Grão: **Servidor × Mês** (uma linha por `cpf_norm` + `posicao`)

| Campo | Tipo | Descrição |
|---|---|---|
| `cpf_norm` | `string` | CPF do servidor normalizado (apenas números, 11 dígitos). Utilizado como chave principal de consolidação entre os domínios. |
| `posicao` | `string` | Posição mensal da base no formato `YYYY-MM`. Representa o snapshot do servidor naquele mês. |
| `remuneracao_basica_bruta_r_num` | `double` | Valor bruto da remuneração básica do servidor no mês, convertido para formato numérico. |
| `remuneracao_apos_deducoes_obrigatorias_r_num` | `double` | Valor da remuneração após deduções obrigatórias (valor líquido base). |
| `irrf_r_num` | `double` | Valor de Imposto de Renda Retido na Fonte no mês. |
| `demais_deducoes_r_num` | `double` | Outras deduções registradas no período. |
| `total_de_verbas_indenizatorias_r_num` | `double` | Total de verbas indenizatórias registradas para o servidor na posição. |
| `grupo_ocupacao` | `string` | Grupo ocupacional ou cargo do servidor, utilizado para segmentação e análises de política de crédito. |
| `tem_afastamento` | `int` | Indicador de afastamento no mês (1 = possui afastamento, 0 = não possui). |
| `qtd_registros_afastamento` | `bigint` | Quantidade de registros de afastamento associados ao servidor na posição. |
| `qtd_observacoes` | `bigint` | Quantidade de observações registradas para o servidor no mês. |
| `source_remuneracao` | `string` | Identificador da origem dos dados de remuneração (linhagem do domínio). |
| `source_cadastro` | `string` | Identificador da origem dos dados de cadastro. |
| `source_afastamento` | `string` | Identificador da origem dos dados de afastamento. |
| `source_observacoes` | `string` | Identificador da origem dos dados de observações. |
| `gold_created_ts` | `timestamp` | Timestamp de geração do registro na camada Gold. |