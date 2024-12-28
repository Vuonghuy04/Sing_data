import yfinance as yf
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

global isWriteHeader
isWriteHeader = True

def get_indicators(comp_code):
    try:
        ticker = yf.Ticker(f"{comp_code}.SI")  # Change .AX to .SI for SGX stocks
        income_stmt = ticker.financials
        years = income_stmt.columns  # Get years in financial statements
        balance_sheet = ticker.balance_sheet

        company_name = ticker.info.get("longName", "N/A")
        sector = ticker.info.get("sector", "N/A")
        industry = ticker.info.get("industry", "N/A")

        for year in years:

            net_income = income_stmt.loc["Net Income", year]  # Net Income - worked
            share_outstanding = ticker.info["sharesOutstanding"]  # Share Outstanding

            if "TTM" in income_stmt.columns:
                basic_EPS = income_stmt.loc["Basic EPS", "TTM"]  # EPS
            else:
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

            # Prepare data to be written to the CSV file
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
            }
            write_to_csv(fin_data)
    except Exception as e:
        print(f"Indicator error: {e}")

def write_to_csv(fin_data):
    output_file = f"fin_data_{fin_data['Year']}.csv"
    with open(output_file, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fin_data.keys())
        if isWriteHeader:
            writer.writeheader()  # Write the header row (column names)
        writer.writerow(fin_data)

    print(f"Data for {fin_data['Year']} successfully written to {output_file}")

isWriteHeader = True

if __name__ == "__main__":
    # Load SGX company data from the CSV
    company_list_file = "company_codes.csv"
    company_data = pd.read_csv(company_list_file)

    # Ensure 'Company Code' column exists
    if "Company Code" not in company_data.columns:
        raise ValueError("The input CSV must have a 'Company Code' column")

    company_data = company_data.head(2000)  # Limit to first 50 rows if needed

    # Extract SGX tickers and loop through them
    for comp_code in company_data["Company Code"]:
        get_indicators(comp_code)
        isWriteHeader = False  # Don't write the header after the first entry

    # Using ThreadPoolExecutor for concurrent processing (optional)
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_indicators, company_data["Company Code"])
