from matplotlib import ticker
import pandas as pd
import sqlalchemy as sqla
import matplotlib.pyplot as plt
from pathlib import Path

MIN = 2014
MAX = 2023

base_dir = Path(__file__).resolve().parent
csv_dir = base_dir / 'csv'
json_dir = base_dir / 'uniao.json'

def faz_json():
    l = []
    for ano in range (MIN,MAX+1):
        csv_path = csv_dir / f'violencia_domestica_{ano}.csv'
        if csv_path.exists():
            temp_df = pd.read_csv(csv_path, sep=';', encoding='latin1')
            l.append(temp_df)
    
    if not l:
        print("Nenhum arquivo CSV encontrado.")
        return
    
    dfCompleto = pd.concat(l, ignore_index=True)
    dfCompleto.to_json(json_dir, orient='records', indent=4)
    print(f"Arquivo JSON criado em: {json_dir}")


def sql():
    dfs_lista = []
    for i in range(MIN, MAX + 1):
        path = csv_dir / f'violencia_domestica_{i}.csv'
        if path.exists():
            dfs_lista.append(pd.read_csv(
                path, sep=';', parse_dates=['data_fato'],
                dtype={'municipio_cod': 'int64', 'municipio_fato': 'string'},
                na_values=['-', 'NA']
            ))

    db_path = base_dir / "novo_banco.sqlite"
    engine = sqla.create_engine(f"sqlite:///{db_path}")    
    
    with engine.connect() as conn:

        conn.execute(sqla.text("DROP TABLE IF EXISTS violencia_domestica"))

        conn.execute(sqla.text("""
            CREATE TABLE IF NOT EXISTS violencia_domestica (
                id INTEGER PRIMARY KEY,
                municipio_cod INTEGER,
                municipio_fato TEXT,
                data_fato DATE,
                mes int,
                ano int,
                risp TEXT,
                rmbh TEXT,
                natureza_delito TEXT,
                tentado_consumado TEXT,
                qtde_vitimas INTEGER
            )
        """))

        for val in dfs_lista:
            val.to_sql("violencia_domestica",conn, if_exists='append', index=False)

        conn.commit()
    print("Banco de dados criado")
    return engine

def grafico(engine):
    with engine.connect() as conn:
        soma = []
        datas = []
        for ano in range(MIN, MAX + 1):
            query = sqla.text("SELECT SUM(qtde_vitimas) FROM violencia_domestica WHERE ano = :ano")
            resultado = conn.execute(query, {"ano": ano}).scalar()
            soma.append(resultado if resultado else 0)
            datas.append(ano)

        #grafico adicional (natureza do delito)
        df_natureza = pd.read_sql("SELECT natureza_delito, COUNT(*) as total FROM violencia_domestica GROUP BY natureza_delito ORDER BY total DESC LIMIT 5", conn)
        plt.figure(figsize=(12, 6))
        
        ax1 = plt.subplot(1, 2, 1)
        plt.plot(datas, soma, marker='o', color='red')
        plt.title('Evolução das Vítimas')
        plt.xlabel('Ano')
        plt.ylabel('Qtd')
        plt.ylim(80000,200000)
        ax1.yaxis.set_major_locator(ticker.MultipleLocator(10000))
        ax1.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        plt.grid(True, alpha=0.3)

        plt.subplot(1, 2, 2)
        plt.bar(df_natureza['natureza_delito'], df_natureza['total'], color='gray')
        plt.title('Top 5 Naturezas de Delito')
        plt.xticks(rotation=45, ha='right', fontsize=8)

        plt.tight_layout()
        plt.show()


def estatisticas(engine):
    with engine.connect() as conn:
        df_db = pd.read_sql(sqla.text("SELECT * FROM violencia_domestica"), conn)
        print("\nESTATÍSTICAS")
        print(df_db.describe())

faz_json()
engine = sql()
grafico(engine)
estatisticas(engine )
