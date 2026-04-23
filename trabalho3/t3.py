import logging
from time import sleep
from matplotlib import ticker
import pandas as pd
import sqlalchemy as sqla
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import errorcode
from pandas.io import sql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MIN = 2014
MAX = 2023
DB_NAME = 'Trabalho3'
DB_CONFIG = {
    'user': 'bah',
    'password': '*******',
    'host': 'localhost'
}

def carrega_dados():
    df = []
    for i in range(MIN, MAX):
        caminho = f'../trabalho2/csv/violencia_domestica_{i}.csv'
        df.append(pd.read_csv(
            caminho,
            sep=';',
            parse_dates=['data_fato'],
            dtype={
                'municipio_cod':'int64',
                'municipio_fato': 'string'
            },
            na_values=['-', 'NA']
        ))
    return df

def create_database(cursor):
    #Cria o banco de dados especificado se ele não existir
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Falha ao criar banco de dados: {}".format(err))
        exit(1)

def configura():
    try:
        # Tenta selecionar o banco de dados
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        # Se o banco de dados não existir (erro ER_BAD_DB_ERROR)
        print("Banco de dados {} não existe.".format(DB_NAME))
        if err.errno ==  errorcode.ER_BAD_DB_ERROR:
            create_database(cursor) # Chama a função para criar o banco de dados
            print("Banco de dados {} criado com sucesso.".format(DB_NAME))
            # Define o banco de dados ativo para a conexão
            cnx.database = DB_NAME 
        else:
            # Outros erros são impressos e o programa é encerrado
            print(err)
            exit(1)

def criaTabela():
    sql = "DROP TABLE IF EXISTS violencia_contra_mulher"
    cursor.execute(sql)
    TABLE_SCHEMA = """
    CREATE TABLE `violencia_contra_mulher` (
    `municipio_cod` INT(6),
    `municipio_fato` VARCHAR(32),
    `data_fato` DATE,
    `mes` INT(2),
    `ano` INT(4),
    `risp` VARCHAR(80),
    `rmbh` VARCHAR(90),
    `natureza_delito` VARCHAR(100),
    `tentado_consumado` VARCHAR(100),
    `qtde_vitimas` INT(2),
    `id` MEDIUMINT NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_registro` (`municipio_cod`, `data_fato`, `natureza_delito`) 
    ) ENGINE=InnoDB
    """
    # unique key evita duplicar os dados agora que tem o insere 

    cursor.execute(TABLE_SCHEMA)
    print("Tabela criada com sucesso.")

def insereDados(cnx, cursor, lista_df):
    sql = """
    INSERT INTO violencia_contra_mulher 
    (municipio_cod, municipio_fato, data_fato, mes, ano, risp, rmbh, natureza_delito, tentado_consumado, qtde_vitimas)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    qtde_vitimas = VALUES(qtde_vitimas),
    tentado_consumado = VALUES(tentado_consumado)
    """
    try:
        cnx.start_transaction()
        
        for val in lista_df:
            # trata para evitar que o MySQL bugue com as datas do Pandas
            val['data_fato'] = val['data_fato'].dt.strftime('%Y-%m-%d')
            data = val.values.tolist()
            cursor.executemany(sql, data)

        cnx.commit()
        print(f"Carga finalizada! {cursor.rowcount} registros afetados.")

        # Se for uma linha nova conta como 1
        # Se a linha ja existia e foi atualizada conta como 2
        # Se a linha ja existia mas os dados eram iguias conta como 0

    except mysql.connector.Error as err:
        cnx.rollback()
        print(f"Erro na carga. Transação cancelada para manter a integridade: {err}")
        raise 

def SlecionaDados(cursor):
    sql = "SELECT * FROM violencia_contra_mulher"
    cursor.execute(sql)
    resultado = cursor.fetchall()
    # for linha in resultado_violencia_contra_mulher:
    #     print(linha)
    print(f"Consulta realizada! Total de linhas recuperadas: {len(resultado)}")
    return resultado

def grafico(df, cursor, cnx):
    soma = []
    datas = []
    for ano in range(MIN, MAX + 1):
        query = "SELECT SUM(qtde_vitimas) FROM violencia_contra_mulher WHERE ano = %s"
        cursor.execute(query, (ano,))
        resultado = cursor.fetchone()[0]
        soma.append(resultado if resultado else 0)
        datas.append(ano)

    df_natureza = pd.read_sql("SELECT natureza_delito, COUNT(*) as total FROM violencia_contra_mulher GROUP BY natureza_delito ORDER BY total DESC LIMIT 5", cnx)
    
    plt.figure(figsize=(14, 7))
    
    ax1 = plt.subplot(1, 2, 1)
    plt.fill_between(datas, soma, color="skyblue", alpha=0.4)
    plt.plot(datas, soma, color="Slateblue", alpha=0.6, linewidth=2)
    
    plt.title('Evolução das Vítimas (Área)')
    plt.xlabel('Ano')
    plt.ylabel('Qtd')
    plt.ylim(80000, 200000)
    ax1.yaxis.set_major_locator(ticker.MultipleLocator(10000))
    ax1.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    plt.grid(True, alpha=0.2)

    plt.subplot(1, 2, 2)
    plt.barh(df_natureza['natureza_delito'], df_natureza['total'], color='teal', alpha=0.7)
    
    plt.title('Top 5 Naturezas de Delito (Horizontal)')
    plt.xlabel('Total de Ocorrências')
    plt.gca().invert_yaxis() 
    plt.tick_params(axis='y', labelsize=9)

    plt.tight_layout()
    plt.show()

def estatisticas(df):
    print("\nESTATÍSTICAS")
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True)
    print(df.describe())


meus_dados = carrega_dados()
cnx = None
try:
    cnx = mysql.connector.connect(**DB_CONFIG)
    cursor = cnx.cursor()
    configura()
    criaTabela()
    insereDados(cnx, cursor, meus_dados)
    resultado_final = SlecionaDados(cursor)
    if resultado_final:
        print("\nExemplo da primeira linha:", resultado_final[0])
    grafico(meus_dados, cursor, cnx)
    estatisticas(meus_dados)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        cnx.shutdown()
    else:
        print(err.msg)
finally:
    if cnx.is_connected():
        cursor.close()
        cnx.disconnect()
        cnx.close()
        
# Conexão -------------

# Banco de Dados ----------

# set_database()

# get_database()

# in_transaction()

# config()

# Consultas --------------

# cmd_query()

# cmd_query_iter()

# get_rows() / get_row()

# Outros --------------

# cmd_quit()

# cmd_init_db()

# cmd_query()
