import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix,precision_score, recall_score,f1_score,roc_curve, roc_auc_score
from sklearn.linear_model import SGDClassifier
from sklearn.inspection import DecisionBoundaryDisplay
df = pd.DataFrame()
def plot_decision_boundary(X, y, model):
  # Criando uma malha (grid) para pintar o fundo
  h = .02  # tamanho do passo no mesh
  x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
  y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
  xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

  # Prevendo probabilidades para cada ponto do grid
  Z = np.array(model.predict(np.c_[xx.ravel(), yy.ravel()]))
  Z = Z.reshape(xx.shape)

  plt.figure(figsize=(10, 6))
    
  # Pintando a região de decisão
  plt.contourf(xx, yy, Z, cmap=plt.cm.RdYlGn, alpha=0.3)
    
  # Plotando os pontos reais
  scatter = plt.scatter(X[:, 0], X[:, 1], c=y, edgecolors='k', s=100, cmap=plt.cm.RdYlGn, label="Dados Reais")
    
  # Linha da fronteira (onde prob = 0.5)
  # Equação: w1*x1 + w2*x2 + b = 0  => x2 = -(w1*x1 + b) / w2
  x1_line = np.array([x_min, x_max])
  x2_line = -(model.coef_[0][0] * x1_line + model.intercept_) / model.coef_[0][1]
  plt.plot(x1_line, x2_line, ls="--", color="black", label="Fronteira de Decisão (P=0.5)")

  plt.title("Fronteira de Decisão Linear", fontsize=14)
  plt.xlabel("Ano", fontsize=12)
  plt.ylabel("Quantidade de Vitimas", fontsize=12)
  plt.legend()
  plt.grid(alpha=0.3)
  plt.savefig('fronteira.png')
  plt.show()

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
random_rows = df.sample(n=v['TENTADO'])
df = pd.concat([aux, random_rows], ignore_index=True)

    
pre_scale = df[['qtde_vitimas', 'ano']]

sc = StandardScaler()
scaled = sc.fit_transform(pre_scale)

res = pd.DataFrame(scaled, columns=pre_scale.columns)

cidades = pd.read_csv(
            "cidades.csv",
            index_col="cidade"
)
X = res[['ano', 'qtde_vitimas']]

y = df['tentado_consumado']
y = y.map({'CONSUMADO': 0, 'TENTADO': 1})
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=29
)

scaler = StandardScaler()

X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = LogisticRegression(max_iter=1000, solver='newton-cholesky')

model.fit(X_train, y_train)

y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"accuracy: {round(accuracy,2)}")
cm = recall_score(y_test, y_pred)
print(f"Recall: {cm}")
precision = precision_score(y_test, y_pred)
print(f"Precision: {round(precision,2)}")
f1 = f1_score(y_test, y_pred)
print(f"f1 score: {round(f1,2)}")
y_prob = model.predict_proba(X_test)[:, 1]

fpr, tpr, thresholds = roc_curve(y_test, y_prob)

fig, ax = plt.subplots()
DecisionBoundaryDisplay.from_estimator(
    model, 
    X_test, 
    response_method="predict", 
    cmap=plt.cm.coolwarm, 
    alpha=0.8, 
    ax=ax, 
    xlabel="Ano", 
    ylabel="Quantidade de Vitimas"
)

scatter = ax.scatter(X_test[:, 0], X_test[:, 1], c=y_test, cmap=plt.cm.coolwarm, edgecolors="k")
plt.title("Fronteira de Decisão")
plt.savefig('fronteira.png')

plt.show()

plot_decision_boundary(X_test, y_test, model)


coeficientes = pd.DataFrame({
    "Variavel": X.columns,
    "Coeficiente": model.coef_[0]
})

coeficientes = coeficientes.sort_values(
    by="Coeficiente",
    key=abs,
    ascending=False
)

print("\nIntercepto:", model.intercept_[0])
print("\nCoeficientes mais relevantes:")
print(coeficientes)

