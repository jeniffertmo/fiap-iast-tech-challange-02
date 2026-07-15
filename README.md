# IAST FIAP - Tech Challenge 2

# Contexto do Problema

A alfabetização na infância é essencial para o desenvolvimento educacional e social do país. O Compromisso Nacional Criança Alfabetizada busca garantir que todas as crianças estejam alfabetizadas até o 2º ano do fundamental. A Pesquisa Alfabetiza Brasil (2023) definiu o ponto de corte de 743 pontos na escala Saeb como referência de proficiência. Com base nisso, foi criado o Indicador Criança Alfabetizada, que mede o percentual de estudantes que atingem esse nível. A integração de metas nacionais, estaduais, municipais e microdados educacionais é necessária para análises profundas e políticas públicas baseadas em evidências. 

## Desafio Educacional

A construção de um pipeline híbrido para análise da alfabetização no Brasil combina batch para consolidar grandes volumes de microdados e aplicar regras de qualidade com streaming para capturar eventos em tempo real, como presenças e avaliações. Essa integração garante dados confiáveis, auditáveis e atualizados, permitindo insights acionáveis para gestores educacionais acompanharem continuamente o desafio da alfabetização infantil. 

### Indicador de Alfabetização
Existem 3 indicadores que são gerados na camada gold do pipeline disponíveis para utilização em camadas de consumo como Power BI, Looker, Tableau, Excel, etc. Abaixo uma pequena descrição de cada um deles:

**Indicador de Alfabetização por Município:**

Mede a taxa de alfabetização real em cada município, mostrando quantos alunos avaliados estão alfabetizados. Essa visão permite acompanhar o desempenho local e identificar municípios com maiores desafios ou avanços.

**Comparativo Metas vs Resultados:**

Compara os resultados reais de alfabetização com as metas estabelecidas para cada município. Indica se a meta foi atingida, ficou abaixo ou ainda não foi definida. É essencial para monitorar políticas públicas e avaliar se os objetivos planejados estão sendo cumpridos.

**Evolução Temporal por Estado:**
Analisa a evolução da taxa de alfabetização ao longo dos anos em cada estado. Essa visão mostra tendências regionais e permite identificar se há progresso contínuo ou estagnação, apoiando decisões estratégicas em nível estadual.

## Aplicações em IA
Com os dados da camada Gold, é possível aplicar IA para apoiar decisões educacionais.
- Criar modelos preditivos para identificar municípios com risco de não atingir metas.
- Usar clustering para agrupar regiões com padrões semelhantes de alfabetização.
- Desenvolver recomendações personalizadas de políticas públicas.
- Projetar cenários futuros com séries temporais.
- Integrar dashboards inteligentes que respondem em linguagem natural.

# Descrição Técnica
A construção de um pipeline híbrido para análise da alfabetização no Brasil combina processamento batch e streaming para transformar dados brutos em insights acionáveis:
- Batch (ETL tradicional): usado para consolidar grandes volumes de microdados educacionais, aplicar regras de qualidade, gerar arquivos Parquet particionados e alimentar as camadas Bronze, Silver e Gold.
- Streaming (tempo real): captura eventos contínuos, como atualizações de presença ou resultados de avaliações, permitindo monitoramento imediato de indicadores críticos.
- Quality Check integrado: garante que apenas dados confiáveis sejam gravados no S3, evitando que inconsistências cheguem às análises.
- Logs e auditoria: o uso de CloudWatch e particionamento temporal assegura rastreabilidade e governança.
- Consumo inteligente: com Glue Crawler e Athena, os dados ficam disponíveis para dashboards e análises dinâmicas, apoiando gestores na tomada de decisão rápida frente ao desafio educacional.

## Estrutura do Repositório

```sh
.
├── glue/        - pasta com os arquivos de glue jobs dividido em camadas
│   ├── bronze
│   ├── silve
│   └── gold
├── infra/       - infraestrutura como código dividido em ambientes  
│   ├── monitoring
│   ├── dev
│   └── prd
└── src/         - bibliotecas para usar com o glue
```
## Arquitetura da Solução

### Diagrama da Arquitetura
<img width="1254" height="582" alt="image" src="https://github.com/user-attachments/assets/6ac52754-9db6-4bdb-974c-02e5fa66a2bd" />

### Diagrama da Pipeline
<img width="1226" height="622" alt="image" src="https://github.com/user-attachments/assets/91ca51b2-9421-4fe8-a394-dcdd95b371f5" />

### Decisões de Arquitetura
A arquitetura do pipeline educacional foi construída com foco em qualidade, segurança e governança. As principais decisões incluem:
- Camadas Bronze, Silver e Gold: garantem evolução gradual da qualidade, desde ingestão bruta até visões analíticas confiáveis.
- Glue Job (PySpark): responsável por aplicar regras de negócio, validações de qualidade e gerar visões analíticas, com execução distribuída e escalável. A escolha por Glue Jobs ao invés de um Apache Spark, visa um menor demanda operacional, ao optar por uma ferramenta gerenciada pela provedora de nuvem, e menor custo, já que é totalmente sob demanda, isto é, não consome recursos em idle.
- Arquivos Parquet particionados por data: otimizam consultas e permitem auditoria temporal detalhada, criando estrutura física organizada no S3.
- Quality Check rigoroso: impede gravação de dados inconsistentes, assegurando confiança nos indicadores.
- Glue Crawler: automatiza a catalogação dos esquemas em cada camada, integrando facilmente com Athena.
- IAM Role dedicado: controla permissões de acesso ao S3 e Glue, aplicando o princípio de menor privilégio.
- Logs no CloudWatch: cada etapa gera registros detalhados para rastreabilidade, monitoramento e auditoria contínua.
- Consumo via Athena: possibilita consultas SQL diretas sobre os dados Gold, sem movimentação adicional, simplificando o acesso para BI, dashboards e análises estratégicas.
Em conjunto, essas escolhas criam uma arquitetura modular, auditável e confiável, que transforma dados educacionais brutos em insights estratégicos para apoiar políticas públicas.

## Fluxo de Dados

## Sustentação
A sustentação desse pipeline é fortalecida pelo monitoramento configurado: sempre que um job do AWS Glue falha um alerta por e‑mail é enviado aos responsáveis. Esse fluxo garante visibilidade imediata, resposta ágil e redução de impactos, sustentando a confiabilidade e continuidade dos processos de dados. 

### Monitoramento
Temos uma monitoração que foi configurada para acompanhar falhas em execuções do AWS Glue. A regra no EventBridge detecta mudanças de estado de jobs (FAILED, TIMEOUT, STOPPED) e envia esses eventos para um tópico SNS, que por sua vez notifica os responsáveis por e‑mail.

### FinOps
Os scripts foram desenvolvidos com foco em eficiência operacional e FinOps, trazendo ganhos claros de performance e redução de custos. Um ponto importante da arquitetura é a criação de Glue Jobs sob demanda, que evita execuções contínuas e desnecessárias, acionando o processamento apenas quando há novas cargas de dados ou necessidade de atualização. Além disso, o uso de arquivos Parquet particionados por data permite consultas mais rápidas e econômicas no Athena, já que apenas as partições relevantes são lidas.

O particionamento dinâmico e o repartition aplicado na tabela de alunos reduzem custos de escrita e leitura no S3, evitando desperdício de recursos. Os checks de qualidade garantem que apenas dados confiáveis sejam persistidos, evitando retrabalho e gastos extras com reprocessamento. O uso de cache no Spark durante as validações diminui acessos repetidos ao S3, otimizando consumo e tempo de execução. Por fim, a integração com CloudWatch para logging e auditoria assegura rastreabilidade sem necessidade de ferramentas externas, reduzindo complexidade e custos adicionais.

Em resumo, os scripts foram desenhados não só para serem seguros e governáveis, mas também para serem financeiramente eficientes, alinhando-se às práticas de FinOps e garantindo que cada recurso computacional seja utilizado de forma inteligente.


