import yfinance as yf
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import math
import pandas

global isWriteHeader
isWriteHeader = True


def get_indicators(comp_code):
    try:
        ticker = yf.Ticker(comp_code)
        income_stmt = ticker.financials
        years = income_stmt.columns  # Get years in fin stm
        balance_sheet = ticker.balance_sheet

        company_name = ticker.info.get("longName", "N/A")
        sector = ticker.info.get("sector", "N/A")
        industry = ticker.info.get("industry", "N/A")

        for year in years:

            net_income = income_stmt.loc["Net Income", year]  # Net Income - worked
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

            print(f"\nYear: {year.year}")
            print(f"Company Name: {company_name}")
            print(f"Sector: {sector}")
            print(f"Industry: {industry}")
            print(f"Net Income: {net_income}")
            print(f"Share Outstanding: {share_outstanding}")
            print(f"EPS: {basic_EPS}")
            print(f"BVPS: {bvps}")
            print(f"ROA: {ROA}")
            print(f"ROE: {ROE}")
            print(f"DIV: {dividends_by_year[year.year]}")
            print(f"P/E Ratio {pe_ratio}")
            print(f"Closed price: {year_end_prices[year.year]}")
            print(f"DAR: {DAR}")
            print(f"MB: {MB}")
            print(f"SIZE: {SIZE}")
            print(f"DY: {DY}")

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
        print(f"Idicator error: {e}")


def write_to_csv(fin_data):
    output_file = f"fin_data_{fin_data["Year"]}.csv"
    with open(output_file, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fin_data.keys())
        if isWriteHeader:
            writer.writeheader()  # Write the header row (column names)
        writer.writerow(fin_data)

    print(f"Data for {fin_data["Year"]} successfully written to {output_file}")


isWriteHeader = True
if __name__ == "__main__":
    for comp_code in range(600000, 604000):
        get_indicators(f"{comp_code}.SS")
        isWriteHeader = False
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_indicators, comp_code)