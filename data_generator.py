import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# setup parameters
NUM_USERS = 5000
NUM_ORDERS = 25000
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31)

# criar tabela de usuarios
np.random.seed(42)
user_ids = range(1, NUM_USERS + 1) # ID sequencial até 5000
signup_dates = [START_DATE + timedelta(days = np.random.randint(0,365)) for _ in range(NUM_USERS)] # DATA DE CADASTRO ALEATORIA ENTRE 1 ANO

# gerar dataframe de usuarios

users_df = pd.DataFrame({
    'user_id': user_ids,
    'created_at': signup_dates,
    'platform': np.random.choice(['iOS', 'Android', 'Web'], NUM_USERS, p=[0.4, 0.5, 0.1]) # randomizar de acordo com probabilidade
})

users_df.to_csv('users_bronze.csv', index=False)

print("users_bronze.csv gerado com sucesso!")

# tabela de pedidos

order_user_ids = np.random.choice(user_ids, NUM_ORDERS) # permite multiplos pedidos por usuario
order_dates = []
order_values = []
order_status = []

for uid in order_user_ids: 
    user_signup = users_df.loc[users_df['user_id'] == uid, 'created_at'].values[0]
    user_signup_dt = pd.to_datetime(user_signup)
    
    # Simula compras após o cadastro
    days_active = (END_DATE - user_signup_dt).days
    if days_active > 0:
        order_date = user_signup_dt + timedelta(days=np.random.randint(0, days_active))
    else:
        order_date = user_signup_dt
        
    order_dates.append(order_date)
    order_values.append(round(np.random.normal(55.0, 15.0), 2)) # Ticket médio simulado
    order_status.append(np.random.choice(['completed', 'canceled'], p=[0.92, 0.08]))

orders_df = pd.DataFrame({
    'order_id': range(1, NUM_ORDERS + 1),
    'user_id': order_user_ids,
    'order_date': order_dates,
    'total_amount': order_values,
    'status': order_status
})

# injetar valores nulos para simular dados incompletos
orders_df.loc[np.random.choice(orders_df.index, 50), 'total_amount'] = np.nan

orders_df.to_csv('orders_bronze.csv', index=False)
print("orders_bronze.csv gerado com sucesso!")