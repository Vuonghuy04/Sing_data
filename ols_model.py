import pandas as pd
import statsmodels.api as sm

# Load the dataset
df = pd.read_csv("cleaned_hk_fin_data_2022.csv")

# Define dependent variable (Year End Price) and independent variables
y = df["Year end price"]  # Dependent variable
x = df[
    [
        "EPS",
        "BVPS",
        "ROA",
        "ROE",
        "DIV",
        "DAR",
        "MB",
        "DY",
        "P/E Ratio",
        "Market Cap",
        "Total Assets",
    ]
]  # Independent variables

# Add a constant to the independent variables for the intercept
x = sm.add_constant(x)

# Fit the OLS model
result = sm.OLS(y, x).fit()

# Print the summary of results
print(result.summary())