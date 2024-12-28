import pandas as pd
import os

input_file = "hk_fin_data_2022.csv"

data = pd.read_csv(input_file, encoding='ISO-8859-1')
data.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
data_cleaned = data.dropna()
data_cleaned = data_cleaned.drop_duplicates(subset="Company code")
data_cleaned.reset_index(drop=True, inplace=True)

output_file = f"cleaned_{os.path.basename(input_file)}"
data_cleaned.to_csv(output_file, index=False)

print(f"Cleaned data saved to {output_file}")
