import yfinance as yf
import csv
import os
from concurrent.futures import ThreadPoolExecutor

# Function to get financial indicators for each company
def get_indicators(comp_code):
    try:
        ticker = yf.Ticker(comp_code)
        income_stmt = ticker.financials
        years = income_stmt.columns  # Get years in financial statements
        balance_sheet = ticker.balance_sheet

        company_name = ticker.info.get("longName", "N/A")
        sector = ticker.info.get("sector", "N/A")
        industry = ticker.info.get("industry", "N/A")

        for year in years:
            net_income = income_stmt.loc["Net Income", year]
            share_outstanding = ticker.info["sharesOutstanding"]  # Share Outstanding

            basic_EPS = income_stmt.loc["Basic EPS", year]  # EPS

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

            # Print data for debugging
            print(f"Year: {year.year} | Company: {company_name}")

            fin_data = {
                "Company code": comp_code,
                "Company Name": company_name,
                "Sector": sector,
                "Industry": industry,
                "Year": year.year,
                "EPS": basic_EPS,
                "BVPS": bvps,
                "ROA": ROA,
                "ROE": ROE,
                "DIV": dividends_by_year[year.year],
                "P/E Ratio": pe_ratio,
                "DAR": DAR,
                "MB": MB,
                "DY": DY,
                "Market Cap": SIZE,
                "Total Assets": total_assets[year],
                "Year end price": f"{year_end_prices[year.year]}"
            }
            write_to_csv(fin_data)
    except Exception as e:
        print(f"Indicator error for {comp_code}: {e}")

# Function to write financial data to CSV
def write_to_csv(fin_data):
    output_file = f"hk_fin_data_{fin_data['Year']}.csv"
    
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
    # Generate list of company codes (Hong Kong stocks are usually formatted like '0001.HK', '0700.HK', etc.)
    company_codes = [str(comp_code).zfill(4) + ".HK" for comp_code in range(1700, 2000)]

    # Using ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_indicators, company_codes)

    print("All data processing complete.")
