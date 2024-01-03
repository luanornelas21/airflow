# **Estudo sobre Airflow**
## **Objetivo**
Criar uma pipeline para movimentar dados de 3 fontes diferentes para um único banco de dados, para ter as vendas, funcionários e categorias em um só lugar. Além disso, as fontes de dados recebem dados periodicamente, então será necessário atualizar diariamente o novo banco de dados.
## **Fontes de dados**  
•	Banco PostgreSQL com dados de vendas  
•	API com dados de funcionários  
•	Arquivo parquet com dados de categoria  
## **Ferramentas utilizadas**  
•	Docker – Contêiners para rodar Airflow e PostgreSQL;   
•	Apache Airflow – Para acionar o pipeline automaticamente;  
•	Python – Para construção do pipeline: Extrair, tratar/transformar e armazenar os dados no banco de dados;  
•	PostgreSQL – Banco de dados que irá receber os dados extraídos e tratados;  
## **Desenvolvimento** 
### 1. Docker ###
![image](https://github.com/luanornelas21/airflow/assets/101593894/5da12578-f4b4-4aa6-8ed6-58848729c42f)
### 2. Pipeline ###
Foi criada uma DAG para orquestrar as tasks:
- Criando tabelas no banco de dados:  
	create_table -> Para criar as tabelas de venda, funcionários e categoria no banco de dados, utilizando o PostgreSQL:
- Extraindo e tratando dados:
  - extract_venda -> Para extrair dados do Banco PostgreSQL. Utilizei o PythonOperator e criei uma função para comunicação com a base de dados, extração dos dados e criação de um arquivo .csv para armazenamento dos dados;
  - extract_func -> Para extrair dados da API. Optei também pelo PythonOperator e criei uma função para extração e criação de um arquivo .csv para armazenamento dos dados;
  - extract_parquet -> Para extrair dados de um arquivo Parquet. Utilizei PythonOperator e criei uma função para extração e criação de um arquivo .csv para armazenamento dos dados. Obs: Estava tendo algum bug ao transformar o dataframe do parquet para um arquivo .csv, então transformei cada coluna em uma lista e criei um dataframe. A partir disso, gerei o arquivo .csv;
- Armazenamento dos dados:  
  store_venda -> Para armazenar os dados utilizei o PythonOperator e criei uma função para armazenar o arquivo .csv de venda gerado na tabela de venda do banco de dados. Nessa função utilizo PostgresHook, pois usei o PostgreSQL para o novo banco. Nele aponto o conn_id e utilizo copy_expert(), no qual indico o que precisa ser feito - a tabela que receberá os novos dados e de onde vem eles (arquivo .csv).
  Da mesma forma fiz para os dados de funcionários e categoria.
- Deletando tabelas:  
  delete_table -> Não foi a melhor alternativa, mas como os dados precisam ser armazenados diariamente e alguns deles são os mesmos, estava tendo um problema de o banco armazenar dados repetidos. Uma alternativa foi de deletar as tabelas sempre que o airflow acionar a pipeline. Dessa forma, seria como se fossem reescritas as tabelas, visto que as fontes de dados podem receber dados novos, porém não indica que perdem dados antigos. Acredito que tem outras formas mais otimizadas, mas ainda não conheço.
### 3. Comunicação com banco de dados ###
O banco de dados que utilizei para armazenar os dados tratados foi o PostgreSQL. Utilizei um script para criar contêiners no Docker. Tanto para rodar o Airflow, como o PostgreSQL. E para comunicação da pipeline com o banco de dados para armazenar os dados nele foi por meio do airflow.
### 4. Apache Airflow ###
A partir da pipeline ordenei as tasks:
![image](https://github.com/luanornelas21/airflow/assets/101593894/0c165fd3-3936-4ed0-97c3-cd1608d3af36)
### 5. PostgreSQL ### 
![image](https://github.com/luanornelas21/airflow/assets/101593894/b9c6e7e5-1f7b-49b5-b2bc-d44a43268dca)
![image](https://github.com/luanornelas21/airflow/assets/101593894/b4d4588f-e5be-4f19-9a4a-fce6c2b2ff2a)
![image](https://github.com/luanornelas21/airflow/assets/101593894/e46d01bc-ac38-4dda-b8d1-8be14f1021f2)
![image](https://github.com/luanornelas21/airflow/assets/101593894/a15b0940-e2ff-4d0f-b77d-305d37acc138)
## Conclusão ##
Concluindo esse relatório e desafio, o objetivo foi alcançado, no qual consistia em extrair dados de 3 fontes diferentes e armazená-los em um banco de dados diariamente. 
Entretanto, foi visto alguns pontos para melhoria:  
   •	delete_table: Acredito que há formas mais eficientes e otimizadas para evitar os dados duplicados;     
   •	Task extract: Poderia separar o tratamento dos dados para .csv da extração de dados. Ou seja, ter uma task apenas para extrair os dados e outra para transformar/tratar esses dados antes de armazenar;  
   •	Criar grupos de tasks de extração, transformação e armazenamento. Por exemplo, poderia criar um código apenas de extração (das 3 fontes) e ao invés de ter na principal todas as task de extract, apenas chamaria o grupo de extract;  
   •	Utilizar outros operators, como o SimpleHttpOperator, para extrair os dados de funcionários ou o PostgreOperator para extrair os dados do PostgreSQL;  
   •	Extrair os dados de forma mais otimizada;  
Além disso, acredito que há outras formas de melhorar esse trabalho e que vou buscar aprender e entender melhor.

