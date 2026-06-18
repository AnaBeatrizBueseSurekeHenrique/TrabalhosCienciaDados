from tqdm import tqdm
import pandas as pd
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
import matplotlib.pyplot as plt  
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score
import geobr
from shapely.geometry import Point
import geopandas as gpd
from sklearn.model_selection import cross_val_score
df = pd.DataFrame()
for i in range(14, 24):
    print(f"Loop - {i}")
    inicio = pd.read_csv(
                f"trabalho2\\csv\\violencia_domestica_20{i}.csv",
                sep=';',
                parse_dates=['data_fato'],
                dtype={
                    'municipio_cod':'int64',
                },
                na_values=['-', 'NA'],
                
    )
    df = pd.concat([inicio, df], ignore_index=True)
df = df[df['tentado_consumado'] != "PREENCHIMENTO OPCIONAL"]
print(df['tentado_consumado'].value_counts())
v = df['tentado_consumado'].value_counts()
aux = df[df['tentado_consumado'] != "CONSUMADO"]
random_rows = df.sample(n=v['TENTADO'], random_state=42)
df = pd.concat([aux, random_rows], ignore_index=True)

teste = []

for i in range(0, len(df.values)):
    if(df.iloc[i]["natureza_delito"] not in teste):
        teste.append(df.iloc[i]["natureza_delito"])
    df.at[i, "natureza_delito"] = teste.index(df.iloc[i]["natureza_delito"])
pre_scale = df[['mes', 'qtde_vitimas', "natureza_delito"]]

sc = StandardScaler() 
scaled = sc.fit_transform(pre_scale)

res = pd.DataFrame(scaled, columns=pre_scale.columns)
res['latitude'] = res['mes']*0
res['longitude'] = res['mes']*0

cidades = pd.read_csv(
            "cidades.csv",
            
            index_col="cidade"
)
for i in range(0, len(df.values)):
    res.at[i, 'latitude'] = cidades.loc[df.loc[i]['municipio_fato']]['latitude']
    res.at[i,'longitude'] = cidades.loc[df.loc[i]['municipio_fato']]['longitude']

x = res[['mes', 'qtde_vitimas', 'latitude', 'longitude', "natureza_delito"]]
y = df['tentado_consumado']
y = y.map({'CONSUMADO': 0, 'TENTADO': 1})
print(len(y))
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
melhor, val = 0, 0

for i in range(1, 15):
    knn = KNeighborsClassifier(n_neighbors=i)
    knn.fit(x_train,y_train)

    y_pred = knn.predict(x_test)
    scores = cross_val_score(knn, x, y, cv=5, scoring='accuracy')
    print(scores)
    if(max(scores) > val):
        val = max(scores)
        melhor = i
    print(f"Accuracy: {scores}")
    precision = precision_score(y_test, y_pred)
    print(f"Precision: {precision}")
minas_gerais = geobr.read_municipality(code_muni="MG", year=2022)

print(f"MELHOR: {melhor} SCORE = {val} ")
knn = KNeighborsClassifier(n_neighbors=melhor)
knn.fit(x_train,y_train)

y_pred = knn.predict(x_test)

fig, ax = plt.subplots(figsize=(40,40))
minas_gerais.plot(ax=ax, facecolor="#f2f2f2", edgecolor="#999999") 
vals = {
    0 : {
        "cor": "red",
        "marcado": False,
        "nome": "CONSUMADO"
    }, 
    
    1 : {
        "cor": "blue",
        "marcado": False,
        "nome": "TENTADO"
    }, 
    
}
for i in tqdm(range(len(y_test.values)), desc="Criando Mapa..."):
    
    ponto = [Point(x_test.values[i][3], x_test.values[i][2])]
    point_gdf = gpd.GeoDataFrame(geometry=ponto, crs="EPSG:4674")

    
    if(vals[y_test.values[i]]["marcado"]):

        point_gdf.plot(ax=ax, color=vals[y_test.values[i]]["cor"], markersize=100, marker="o")
    else:
        print()
        point_gdf.plot(ax=ax, color=vals[y_test.values[i]]["cor"], markersize=100, marker="o", label=vals[y_test.values[i]]["nome"])
        vals[y_test.values[i]]["marcado"] = True


ax.set_title("Predição de Violência Contra Mulher Tentada Ou Consumada", fontsize=14)
ax.axis("off")
plt.legend()
plt.savefig('knn_estados.png')
plt.show()

