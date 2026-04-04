from time import sleep

import numpy
MIN = 2014
MAX = 2017
from matplotlib.ticker import MultipleLocator
import pandas as pd
import sqlalchemy as sqla
import matplotlib.pyplot as plt
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
engine = sqla.create_engine("sqlite:///novo_banco.sqlite")

with engine.connect() as conn:
    conn.execute(sqla.text("""
        CREATE TABLE IF NOT EXISTS violencia_domestica (
            id INTEGER PRIMARY KEY,
            municipio_cod INTEGER,
            municipio_fato TEXTO,
            data_fato DATE,
            mes int,
            ano int,
            risp TEXT,
            rmbh TEXT,
            natureza_delito TEXT,
            tentado_consumado TEXT,
            qntde_vitimas INTEGER
        )
    """))
    i = 0
    for val in df:
        if(i == 0):
            val.to_sql("violencia_domestica",conn, if_exists='replace')
            i +=1
        else:
            val.to_sql("violencia_domestica",conn, if_exists='append')
    conn.commit()
    soma = []
    datas = []
    for i in range(MIN, MAX):
        val = pd.read_sql(f"SELECT * from violencia_domestica WHERE ano == {i}", conn)
        soma.append(val['qtde_vitimas'].sum())
        datas.append(i)
    print(soma)
    plt.plot(datas, soma)
    plt.xticks(numpy.arange(MIN, MAX, 1.0))
    plt.ylabel('Quantidade de Vítimas')
    plt.xlabel('Ano')
    plt.show()
    aux = pd.read_sql(f"SELECT * from violencia_domestica", conn)
    print(aux.describe())