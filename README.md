# Code Challenge: Data Engineer

## Como executar o projeto

1. Instalar UV (ou Poetry):
    - https://docs.astral.sh/uv/getting-started/installation/
2. Executar os comandos:
    ```bash
    uv install
    cd dags/etl
    uv run python json_to_duckdb.py 
    ``` 
    > se quiser pode excluir os arquivos:
        - LoggingEtl.log
        - payments.crypt.parquet
        - payments.parquet
        - transactions.parquet
3. Para executar os tests:
    ```bash
    uv run pytest
    ```

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

### Respostas

A solução desenvolvida utiliza o DuckDB como intermediário e arquivos .parquet como saída para os dados, a escolha por arquivos parquet vem da facilidade de leitura desses arquivos por diferentes ferramentas, enquanto ainda mantém uma estrutura tabular dos dados.
Ao utilizar o DuckDB também permitimos a criação de limites de memória disponivel permitindo o sistema rodar em locais com pouca memória.

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

### Respostas

**Como controlar quem pode acessar o que, e como controlar as permissões de acesso?**
1. Primeiramente classificar os dados de acordo com sua "sensibilidade", inicialmente podemos adotar um padrão simples e a medida que os dados evoluem podemos também evoluir os grupos:
    - Geral: os dados que podem ser acessados por todos da empresa
    - Anônimo: os dados que podem ser acessados por todos da empresa somente após a anonimização de '_PII_'
    - Restrito: Dados que só podem ser acessados com autorização explícita
2. Após a criação dos grupos precisamos classificar os dados de acordo com cada um desses grupos, essa classificação pode ser feita a nível de Tabela ou mesmo a nível de Coluna, por exemplo:
    - Classificação a nível de Tabela:
        - A Tabela 'customers' é classificada como 'Restrito', já a tabela 'transactions' é classificada como 'Anônimo' e por fim a tabela 'products' é classificada como 'Geral'
    - Classificação a nível de Coluna:
        - A tabela 'customers' possui as seguintes colunas: 'cpf', 'nome', 'email', 'telefone', 'idade', 'sexo', 'id'. Dessa forma poderíamos classificar como:
            - Restrito: 'nome', 'email', 'telefone'
            - Anônimo: 'id', 'idade', 'sexo', 'cpf'
        - Já a tabela doenças pré-existentes que contem: 'id_cliente', 'nome_doenca', 'cid-11' poderiam ser classificadas como:
            - Geral: 'cid-11', 'nome_doenca'
            - Anônimo: id_cliente
        - Dessa forma permitimos que o time possa extrair estatísticas e fazer algumas correlações simples entre as doenças mais comuns por faixa etária (por exemplo), ao mesmo tempo protegemos a privacidade dos clientes ao não disponibilizar dados como nome, telefone e email
    - Cada uma das escolhas tem suas vantagens e desvantagens, visto que a classificação a nível de tabela exigiria a criação de tabelas específicas para cada necessidade de negócio porém garante total separação entre as informações.
    - Já a classificação a nível de coluna pode ser mais complexo, e nem todos os sistemas aceitam esse tipo de gestão de acesso.
3. O controle de acesso as informações podem ser feitos:
    - No Banco de Dados, bancos de dados modernos, como o postgres, permitem a configuração de acesso em diferentes granularidades
    - No DataWarehouse, sistemas como Snowflake, BigQuery e outros também permitem o controle de acesso granular as informações armazenadas nos mesmos.
4. Para a anonimização dos dados, existem várias alternativas, que podem ser utilizadas em conjunto, como:
    - Utilização de ferramentas que permitam a anonimização, como por exemplo: https://github.com/nucleuscloud/neosync
    - Separação de informações Protegidas em tabelas diferentes ou criação de views com filtro dessas informações
    - Redução da quantidade de dados privados que são solicitados aos clientes
    - Aplicação de "Hash" em colunas com dados sensíveis
    - Criptografia para proteção dos dados

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

### Resposta
- Para executar o airflow localmente você deve:
0. Acessar a pasta infra:
```bash
cd infra
```
1. Executar o airflow-init para criar o usuário e rodar as migrations dos bancos de dados
```bash 
docker compose up airflow-init
```
2. Limpar o seu ambiente
```bash
docker compose down --volumes --remove-orphans
```
3. Para iniciar o airflow:
```bash
docker compose up -d
```

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
