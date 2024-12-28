import pandas as pd

# Load the CSV file
input_file = "companies-list.csv"
data = pd.read_csv(input_file)

# Define the number of rows per file
rows_per_file = 500

# Get the number of chunks needed to split the data
num_chunks = len(data) // rows_per_file + (1 if len(data) % rows_per_file > 0 else 0)

# Split the data and save each chunk as a new CSV file
output_files = []
for i in range(num_chunks):
    start_idx = i * rows_per_file
    end_idx = (i + 1) * rows_per_file
    chunk = data[start_idx:end_idx]
    
    output_file = f"companies_list_part_{i+1}.csv"
    chunk.to_csv(output_file, index=False)
    output_files.append(output_file)

output_files
