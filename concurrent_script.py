import yfinance as yf
import csv
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor

# Function to fetch balance sheet, income statement, and dividend data for a given ticker
def fetch_ticker_data(comp_code):
    try:
        fetch_obj = yf.Ticker(f"{comp_code}.AX")  # Using .AX suffix for ASX stocks
        blc_sheet = fetch_obj.get_balance_sheet(as_dict=True, pretty=True, freq="yearly")
        imc_stm = fetch_obj.get_income_stmt(as_dict=True, pretty=True, freq="yearly")
        info = fetch_obj.info
        dividends = fetch_obj.dividends  # Fetch dividend data
        
        # Fetch additional company information
        company_name = info.get('longName', 'N/A')
        sector = info.get('sector', 'N/A')
        industry = info.get('industry', 'N/A')

        return blc_sheet, imc_stm, info, dividends, company_name, sector, industry
    except Exception as e:
        print(f"Error fetching data for {comp_code}: {e}")
        return {}, {}, {}, {}, 'N/A', 'N/A', 'N/A'

# Function to calculate financial ratios
def calculate_ratios(blc_sheet, imc_stm, info, dividends, year, company_name, sector, industry):
    ratios = {}
    try:
        net_income = imc_stm[year].get("Net Income", None)
        total_assets = blc_sheet[year].get("Total Assets", None)
        total_liabilities = blc_sheet[year].get("Total Liabilities Net Minority Interest", None)
        shareholders_equity = blc_sheet[year].get("Total Equity Gross Minority Interest", None)
        dividends_payable = blc_sheet[year].get("Dividends Payable", None)
        total_debt = blc_sheet[year].get("Total Debt", None)
        share_price = info.get("previousClose", None)
        outstanding_shares = info.get("sharesOutstanding", None)

        # Calculate financial ratios using normal division and handle potential errors
        ratios["EPS"] = net_income / outstanding_shares if net_income and outstanding_shares else "NaN"
        ratios["BVPS"] = shareholders_equity / outstanding_shares if shareholders_equity and outstanding_shares else "NaN"
        ratios["ROA"] = (net_income / total_assets * 100) if net_income and total_assets else "NaN"
        ratios["ROE"] = (net_income / shareholders_equity * 100) if net_income and shareholders_equity else "NaN"
        ratios["DIV"] = dividends_payable / outstanding_shares if dividends_payable and outstanding_shares else "NaN"
        ratios["DAR"] = (total_liabilities / total_assets * 100) if total_liabilities and total_assets else "NaN"
        
        # Replaced SIZE with TOTAL ASSETS
        ratios["TOTAL ASSETS"] = total_assets if total_assets else "NaN"
        
        # Calculate MARKET CAP (Market Capitalization)
        ratios["MARKET CAP"] = share_price * outstanding_shares if share_price and outstanding_shares else "NaN"

        ratios["P/E"] = share_price / ratios["EPS"] if share_price and ratios["EPS"] != "NaN" else "NaN"
        ratios["DY"] = (ratios["DIV"] / share_price * 100) if ratios["DIV"] != "NaN" and share_price else "NaN"
        ratios["MB"] = share_price / ratios["BVPS"] if share_price and ratios["BVPS"] != "NaN" else "NaN"
        
        # Calculate the most recent dividend from the dividend history
        if dividends is not None and not dividends.empty:
            # Use the most recent dividend
            last_dividend = dividends.iloc[-1]  # Most recent dividend
            if outstanding_shares:
                # Calculate Dividends Per Share (DPS) correctly
                dps = last_dividend / outstanding_shares
                ratios["DIV"] = dps  # Use DPS for dividend
                ratios["DY"] = (dps / share_price * 100) if dps and share_price else "NaN"
        
        # Add company information to the ratios dictionary
        ratios["Company Name"] = company_name
        ratios["Sector"] = sector
        ratios["Industry"] = industry
        
    except Exception as e:
        print(f"Error calculating ratios for year {year}: {e}")
        ratios = {key: "NaN" for key in ["EPS", "BVPS", "ROA", "ROE", "DIV", "DAR", "TOTAL ASSETS", "MARKET CAP", "P/E", "DY", "MB", "Company Name", "Sector", "Industry"]}
    return ratios

# Function to write financial data to a CSV file for a specific year
def write_to_csv(comp_code, year, ratios):
    # Extract the year from the Timestamp object
    sanitized_year = str(year.year)

    # File name for the specific year
    output_file = f"Financial_Data_{sanitized_year}.csv"
    headers = ["Company Code", "Date", "Company Name", "Sector", "Industry", "EPS", "BVPS", "ROA", "ROE", "DAR", "DIV", "TOTAL ASSETS", "MARKET CAP", "P/E", "DY", "MB"]

    # Write header if the file does not exist
    try:
        with open(output_file, mode="x", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
    except FileExistsError:
        pass  # File already exists, no need to rewrite headers

    # Write ratios to the CSV file
    with open(output_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # Construct the row using only the keys in the `ratios` dictionary
        row = [comp_code, str(year.date())] + [ratios.get(key, "NaN") for key in headers[2:]]
        writer.writerow(row)
        print(f"Data for {comp_code} in {sanitized_year} written to {output_file}")

def fetch_and_process_data(comp_code):
    blc_sheet, imc_stm, info, dividends, company_name, sector, industry = fetch_ticker_data(comp_code)

    if blc_sheet and imc_stm and info:
        for year in blc_sheet.keys():
            ratios = calculate_ratios(blc_sheet, imc_stm, info, dividends, year, company_name, sector, industry)
            write_to_csv(comp_code, year, ratios)

# Main block to handle concurrent processing
if __name__ == "__main__":
    # Load the list of companies from the provided CSV file
    company_list_file = "companies-list.csv"
    company_data = pd.read_csv(company_list_file)

    if "Ticker" not in company_data.columns:
        raise ValueError("The input CSV must have a 'Ticker' column")

    # Limit to the first 20 companies (you can adjust this to fit your needs)
    company_data = company_data.head(2200)

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=3) as executor:  # Adjust max_workers as needed
        executor.map(fetch_and_process_data, company_data["Ticker"])

        time.sleep(5)  # Optional: To prevent hitting rate limits
