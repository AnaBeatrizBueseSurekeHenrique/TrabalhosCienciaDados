from time import sleep
import pandas as pd
import sqlalchemy as sqla
import matplotlib.pyplot as plt
import numpy
import mysql.connector
from mysql.connector import errorcode
from pandas.io import sql
MIN = 2014
MAX = 2017

df = []
for i in range(MIN, MAX):
    df.append(pd.read_csv(
        f'violencia_domestica_{i}.csv',
        sep=';',
        parse_dates=['data_fato'],
        dtype={
            'municipio_cod':'int64',
            'municipio_fato': 'string'
        },
        na_values=['-', 'NA']
    ))


i = 0
DB_NAME = 'trabalho3' 

# Conecta ao servidor MySQL sem especificar um banco de dados inicial
cnx = mysql.connector.connect(user='host',password='senha')
cursor = cnx.cursor()

def create_database(cursor):
    #Cria o banco de dados especificado se ele não existir
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Falha ao criar banco de dados: {}".format(err))
        exit(1)

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

sql = "DROP TABLE violencia_contra_mulher"
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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
"""
try:
    cursor.execute(TABLE_SCHEMA)
    print("Tabela 'violencia' criada com sucesso.")
    i = 0
    for val in df:        
        sql = "INSERT INTO violencia_contra_mulher (municipio_cod, municipio_fato,data_fato,mes,ano,risp,rmbh,natureza_delito,tentado_consumado,qtde_vitimas) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"
        data = val.values.tolist()
        cursor.executemany(sql, data)
        cnx.commit()
    sql = "SELECT * from violencia_contra_mulher"
    cursor.execute(sql)


    resultado_violencia_contra_mulher = cursor.fetchall()
     
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
