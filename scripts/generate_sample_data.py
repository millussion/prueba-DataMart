import os
import pandas as pd
import numpy as np

os.makedirs('data/raw', exist_ok=True)

# 1. data.csv
df1 = pd.DataFrame({
    'InvoiceNo': ['536365', '536365', 'C536379', '536381', '536382', '536383', '536384'],
    'StockCode': ['85123a ', '71053', '85123A', '22752', '22633', '21730', 'INVALID_FREE'],
    'Description': ['WHITE HANGING HEART T-LIGHT HOLDER', 'White Metal Lantern', 'WHITE HANGING HEART T-LIGHT HOLDER', 'SET 7 BABUSHKA NESTING BOXES', 'Camouflage led torch', 'glass star frosted', 'FREE SAMPLE'],
    'Quantity': [6, 2, -1, 4, 10, -2, 5],
    'UnitPrice': [2.55, 3.39, 2.55, 7.65, 1.95, 4.25, 0.0],
    'InvoiceDate': ['12/01/2023 08:26', '12/01/2023 08:26', '12/01/2023 09:41', '12/01/2023 10:15', '12/01/2023 11:20', '12/01/2023 12:00', '12/01/2023 12:30'],
    'CustomerID': [17850, 17850, 17850, np.nan, 13047, 13047, 15555],
    'Country': ['Colombia', 'Colombia', 'Colombia', 'México', 'Perú', 'Perú', 'Colombia']
})
df1.to_csv('data/raw/data.csv', index=False)

# 2. online_retail_II.csv
df2 = pd.DataFrame({
    'Invoice': ['489434', '489434', 'C489435', '489436', '536365'],
    'StockCode': ['85048', '79323P', '85048', '22041', '85123A'],
    'Description': ['15CM CHRISTMAS GLASS BALL', 'PINK CHERRY BLOSSOM CUP', '15CM CHRISTMAS GLASS BALL', 'RECORD FRAME', 'WHITE HANGING HEART T-LIGHT HOLDER'],
    'Quantity': [12, 6, -2, 10, 6],
    'Price': [6.95, 2.95, 6.95, 2.10, 2.55],
    'InvoiceDate': ['2022-12-01 07:45:00', '2022-12-01 07:45:00', '2022-12-01 08:00:00', '2022-12-01 09:20:00', '2023-12-01 08:26:00'],
    'Customer ID': [13085, 13085, 13085, 13078, 17850],
    'Country': ['Colombia', 'Colombia', 'Colombia', 'México', 'Colombia']
})
df2.to_csv('data/raw/online_retail_II.csv', index=False)
print("Datos creados en data/raw/")