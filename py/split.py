import pandas as pd

fraud_data = 'fraudTest.csv'

df = pd.read_csv(fraud_data, delimiter=',', header=0, index_col=False)

trans_cols = ['trans_num', 'trans_date_trans_time', 'cc_num', 'amt', 'merchant', 'category', 'is_fraud']
client_cols = ['cc_num', 'first', 'last', 'gender', 'street', 'city', 'state', 'zip', 'job', 'dob']
merchant_cols = ['trans_num', 'merchant', 'merch_lat', 'merch_long']

# transaction table
transaction = df[trans_cols].drop_duplicates()
# client table
client = df[client_cols].drop_duplicates()
# merch table
merchant = df[merchant_cols].drop_duplicates()

csv_trans = transaction.to_csv('transaction.csv', index=False)
csv_client = client.to_csv('client.csv', index=False)
csv_merc = merchant.to_csv('merchant.csv', index=False)
print("Data split into transaction, client, and merchant tables.")
