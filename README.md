# Code Challenge: Data Engineer

## Como executar o projeto

1. Instalar UV (ou Poetry):
    - https://docs.astral.sh/uv/getting-started/installation/
2. Executar os comandos:
    - uv install
    - uv run o arquivo em questão

## Contexto

Somos uma empresa que está transformando dados em insights para melhorar a experiência do cliente e otimizar operações. Estamos construindo uma plataforma robusta que integra dados de diversas fontes e permite a análise eficiente e confiável desses dados. Sua tarefa será implementar um pipeline de ETL que processe dados e os torne acessíveis para consulta, análise e visualização.

## Fontes de Dados

Você receberá dois conjuntos de dados:

- **Transações**: Representa operações entre a empresa e seus clientes, incluindo valores transacionados e condições.
- **Pagamentos**: Representa os pagamentos realizados por clientes em relação às transações previamente registradas.

Esses arquivos podem ser baixados nos seguintes links:

- [transactions.json](transactions.json)
- [payments.json](payments.json)

## Definições

- **Transação**: Uma operação comercial que gera obrigações financeiras, como uma compra ou contrato de serviço.
- **Pagamento**: Um registro de que uma obrigação financeira foi quitada.

## Desafio ETL

Como uma empresa orientada por dados, é crucial termos uma infraestrutura que permita o armazenamento, processamento e análise de dados de maneira eficiente e escalável.

Sua tarefa é construir um pipeline de ETL (Extract, Transform, Load) que processe os arquivos fornecidos e os armazene em um formato estruturado, disponível para consulta em SQL. Sua solução deve ser escalável e confiável para lidar com grandes volumes de dados no futuro.

### Requisitos

- Os dados devem ser armazenados de uma forma que possa ser consultada via SQL.
- Garantir que os dados sejam tratados de maneira segura, sem perda de informações.
- A solução deve ser escalável, considerando um aumento de volume de dados.

### Diferencial

- Emule uma solução de streaming baseada nos arquivos fornecidos.

## Governança e Privacidade

### Desafio

Além de processar e armazenar os dados, é fundamental garantir que a privacidade e a governança de acesso estejam devidamente implementadas.

### Sua tarefa

Descreva como você implementaria uma solução de metadados que garante:

- **Governança**: Quem pode acessar o quê? Como controlar permissões de acesso?
- **Privacidade**: Como garantir que os dados sensíveis sejam protegidos (anonimização, criptografia, etc.)?

### Diferencial

- Implemente essa solução no seu pipeline ETL.

## Regras de Negócio

### Contexto

Queremos analisar o comportamento de nossos clientes em relação a pagamentos atrasados. Vamos criar dois critérios para classificar um cliente como inadimplente:

- **Critério 1**: Se um cliente atrasou um pagamento em mais de 30 dias após 3 meses.
- **Critério 2**: Se um cliente teve algum atraso superior a 15 dias em 6 meses.

### Sua tarefa

Implemente uma lógica que avalie os clientes com base nesses critérios para um período de 6 meses e forneça:

- A porcentagem de clientes inadimplentes.
- A lista de inadimplentes e os respectivos pagamentos que violam os critérios.

Armazene os resultados como arquivos Parquet, particionados por "inadimplente = true ou false".

### Requisitos

- Use Apache Spark para processar os dados.
- A solução deve ser confiável e escalável.

## Orquestração

### Contexto

Depois de implementar o pipeline ETL e o processamento, o próximo desafio é garantir que esse fluxo de trabalho seja orquestrado de maneira eficiente e automatizada.

### Sua tarefa

Use Apache Airflow para criar uma orquestração que:

- Execute o pipeline ETL.
- Execute a análise de inadimplência.

A plataforma deve ser robusta o suficiente para lidar com falhas, permitindo que partes do sistema continuem funcionando mesmo que outras falhem.

## Critérios de Avaliação

- **Eficiência**: Avaliaremos a eficiência do pipeline ETL em termos de tempo de execução e uso de recursos.
- **Clareza do Código**: O código deve ser claro e fácil de entender, com nomeação adequada de variáveis e funções.
- **Documentação**: A presença de documentação clara e concisa, incluindo docstrings e comentários, será considerada.
- **Escalabilidade**: A solução deve ser capaz de lidar com grandes volumes de dados.
- **Testes**: A inclusão de testes unitários para validar a funcionalidade do código será um diferencial.

## Entrega

- **Repositório Git**: A solução deve ser entregue através de um repositório Git. Faça um fork deste repositório e envie um pull request com a solução.
- **Prazo**: O prazo para a entrega da solução é de 7 dias a partir do recebimento do desafio.
- **Instruções de Execução**: Inclua um arquivo `README.md` atualizado com instruções claras sobre como configurar e executar a solução.

## Documentação e Comentários

- **Docstrings**: Utilize docstrings para documentar módulos, classes, métodos e funções, seguindo o estilo PEP 257.
- **Comentários**: Inclua comentários no código para explicar a lógica e as decisões de design, focando no "porquê" do código.
- **Exemplo de Uso**: Se possível, inclua exemplos de uso no `README.md` para demonstrar como a solução deve ser executada.
