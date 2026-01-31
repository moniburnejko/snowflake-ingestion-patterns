import pandas as pd

fraud_data = 'fraudTest.csv'

df = pd.read_csv(fraud_data, delimiter=',', header=0, index_col=False)

trans_cols = ['trans_num', 'trans_date_trans_time', 'cc_num', 'amt', 'merchant', 'category', 'is_fraud']
client_cols = ['cc_num', 'first', 'last', 'gender', 'street', 'city', 'state', 'zip', 'job', 'dob']
merchant_cols = ['trans_num', 'merchant', 'merch_lat', 'merch_long']

transaction = df[trans_cols].drop_duplicates()
client = df[client_cols].drop_duplicates()
merchant = df[merchant_cols].drop_duplicates()

trans_csv = transaction.to_csv('transaction.csv', index=False)
client_csv = client.to_csv('client.csv', index=False)
merchant_csv = merchant.to_csv('merchant.csv', index=False)

print("Done! Data split into transaction, client, and merchant csv files.")
