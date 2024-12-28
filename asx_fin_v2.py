import yfinance as yf
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import os

# Global variable for header writing control
isWriteHeader = True

# Function to get financial indicators for each company
def get_indicators(comp_code):
    global isWriteHeader
    try:
        ticker = yf.Ticker(f"{comp_code}.AX")  # Using .AX for ASX stocks
        income_stmt = ticker.financials
        years = income_stmt.columns  # Get years in financial statements
        balance_sheet = ticker.balance_sheet

        company_name = ticker.info.get("longName", "N/A")
        sector = ticker.info.get("sector", "N/A")
        industry = ticker.info.get("industry", "N/A")

        for year in years:
            net_income = income_stmt.loc["Net Income", year]
            share_outstanding = ticker.info["sharesOutstanding"]

            basic_EPS = income_stmt.loc["Basic EPS", year]

            total_stock_equity = balance_sheet.loc["Stockholders Equity"]
            bvps = total_stock_equity[year] / share_outstanding  # BVPS

            total_assets = balance_sheet.loc["Total Assets"]
            ROA = net_income / total_assets[year]  # ROA

            total_equity = balance_sheet.loc["Total Equity Gross Minority Interest"]
            ROE = total_assets[year] / total_equity[year]  # ROE

            dividends = ticker.dividends
            dividends_by_year = dividends.resample("YE").sum()
            dividends_by_year.index = dividends_by_year.index.year  # DIV

            historical_data = ticker.history(period="max")
            year_end_prices = historical_data["Close"].resample("YE").last()
            year_end_prices.index = year_end_prices.index.year
            pe_ratio = year_end_prices[year.year] / basic_EPS  # P/E Ratio

            total_debt = balance_sheet.loc["Total Debt"]
            DAR = total_debt[year] / total_assets[year]  # DAR

            MB = year_end_prices[year.year] / bvps
            SIZE = year_end_prices[year.year] * share_outstanding

            DY = dividends_by_year[year.year] / year_end_prices[year.year]

            fin_data = {
                "Company code": comp_code,
                "Company Name": company_name,
                "Sector": sector,
                "Industry": industry,
                "Year": year.year,
                "EPS": f"{basic_EPS}",
                "BVPS": f"{bvps}",
                "ROA": f"{ROA}",
                "ROE": f"{ROE}",
                "DIV": f"{dividends_by_year[year.year]}",
                "P/E Ratio": f"{pe_ratio}",
                "DAR": f"{DAR}",
                "MB": f"{MB}",
                "DY": f"{DY}",
                "Market Cap": f"{SIZE}",
                "Total Assets": f"{total_assets[year]}",
                "Year end price": f"{year_end_prices[year.year]}"
            }
            write_to_csv(fin_data)
    except Exception as e:
        print(f"Indicator error: {e}")


# Function to write financial data to CSV
def write_to_csv(fin_data):
    global isWriteHeader
    output_file = f"asx_fin_data_{fin_data['Year']}.csv"
    
    # Check if the file already exists
    file_exists = os.path.exists(output_file)

    # Write to CSV with header check
    with open(output_file, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fin_data.keys())
        
        # Write header only if the file doesn't exist yet
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(fin_data)

    print(f"Data for {fin_data['Year']} successfully written to {output_file}")


if __name__ == "__main__":
    # Load ASX company data from the CSV
    company_list_file = "companies_list_part_4.csv"
    company_data = pd.read_csv(company_list_file)

    # Ensure 'Ticker' column exists
    if "Ticker" not in company_data.columns:
        raise ValueError("The input CSV must have a 'Ticker' column")

    # Limit to the first 2000 companies (optional, adjust as needed)
    company_data = company_data.head(500)

    # Using ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_indicators, company_data["Ticker"])

    print("All data processing complete.")
