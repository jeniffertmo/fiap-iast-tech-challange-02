# IAST FAIP - Tech Challange 2

# Contexto do Problema
Pipeline Híbrida para Análise da Alfabetização no Brasil

## Desafio Educacional

### Indicador de Alfabetização
Existem 3 indicadores que são gerados na camada gold do pipeline disponiveis para utilização em camadas de consumo como Power BI, Looker, Tableau, Excel, etc. Abaixo uma pequena descriação de cada um deles:

Indicador de Alfabetização por Município 📊
Mede a taxa de alfabetização real em cada município, mostrando quantos alunos avaliados estão alfabetizados. Essa visão permite acompanhar o desempenho local e identificar municípios com maiores desafios ou avanços.

Comparativo Metas vs Resultados 🎯
Compara os resultados reais de alfabetização com as metas estabelecidas para cada município. Indica se a meta foi atingida, ficou abaixo ou ainda não foi definida. É essencial para monitorar políticas públicas e avaliar se os objetivos planejados estão sendo cumpridos.

Evolução Temporal por Estado 📈
Analisa a evolução da taxa de alfabetização ao longo dos anos em cada estado. Essa visão mostra tendências regionais e permite identificar se há progresso contínuo ou estagnação, apoiando decisões estratégicas em nível estadual.

## Aplicações em IA

# Descrição Técnica

## Estrutura do Repositório

```sh
.
├── glue/        - pasta com os arquivos de glue jobs dividido em camadas
│   ├── bronze
│   ├── silve
│   └── gold
├── infra/       - infraestrutura como código dividido em ambientes  
│   ├── dev
│   └── prd
└── src/         - bibliotecas para usar com o glue
```

## Arquitetura da Solução

- AWS Glue: usado para a execução dos workloads de ETL. References:
    - https://docs.aws.amazon.com/glue/latest/dg/how-it-works.html
    - https://docs.aws.amazon.com/glue/latest/dg/components-overview.html
    - Writing a hello world in AWS Glue https://docs.aws.amazon.com/glue/latest/dg/example_glue_Hello_section.html

- Developing Glue Localy
    - https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-5-0-jobs-locally-using-a-docker-container/



### Diagrama da Arquitetura
<img width="378" height="169" alt="image" src="https://github.com/user-attachments/assets/f63aa521-8c06-4bfc-8ce3-55892eac58a2" />


### Diagrama da Pipeline

### Decisões de Arquitetura

## Fluxo de Dados

## Sustentação

### Monitoramento

### FinOps

