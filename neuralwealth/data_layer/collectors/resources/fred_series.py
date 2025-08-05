# List of dictionaries containing FRED series IDs, names, and descriptions
fred_series = [
    {
        "id": "GDPC1",
        "name": "real_gdp",
        "full_name": "Real GDP",
        "description": "Real Gross Domestic Product (Quarterly, SA)"
    },
    {
        "id": "GDP",
        "name": "nominal_gdp",
        "full_name": "Nominal GDP",
        "description": "Nominal Gross Domestic Product (Quarterly, SA)"
    },
    {
        "id": "GDPDEF",
        "name": "gdp_deflator",
        "full_name": "GDP Deflator",
        "description": "GDP Price Deflator (Quarterly, SA)"
    },
    {
        "id": "CPIAUCSL",
        "name": "cpi_inflation",
        "full_name": "CPI Inflation",
        "description": "Consumer Price Index for All Urban Consumers (Monthly, SA)"
    },
    {
        "id": "CPILFESL",
        "name": "core_cpi",
        "full_name": "Core CPI",
        "description": "Core CPI (excl. Food and Energy, Monthly, SA)"
    },
    {
        "id": "PCEPI",
        "name": "pce_inflation",
        "full_name": "PCE Inflation",
        "description": "Personal Consumption Expenditures Price Index (Monthly, SA)"
    },
    {
        "id": "PCEPILFE",
        "name": "core_pce",
        "full_name": "Core PCE",
        "description": "Core PCE Price Index (excl. Food and Energy, Monthly, SA)"
    },
    {
        "id": "PPIACO",
        "name": "producer_price_index",
        "full_name": "Producer Price Index",
        "description": "PPI for All Commodities (Monthly, NSA)"
    },
    {
        "id": "PPIFGS",
        "name": "core_ppi",
        "full_name": "Core PPI",
        "description": "PPI for Final Goods (Monthly, SA)"
    },
    {
        "id": "UNRATE",
        "name": "unemployment_rate",
        "full_name": "Unemployment Rate",
        "description": "Civilian Unemployment Rate (Monthly, SA)"
    },
    {
        "id": "PAYEMS",
        "name": "nonfarm_payrolls",
        "full_name": "Nonfarm Payrolls",
        "description": "Total Nonfarm Payroll Employment (Monthly, SA)"
    },
    {
        "id": "CIVPART",
        "name": "labor_force_participation",
        "full_name": "Labor Force Participation",
        "description": "Civilian Labor Force Participation Rate (Monthly, SA)"
    },
    {
        "id": "U6RATE",
        "name": "u6_unemployment",
        "full_name": "U-6 Unemployment",
        "description": "Broader Unemployment Rate (Monthly, SA)"
    },
    {
        "id": "CES0500000003",
        "name": "average_hourly_earnings",
        "full_name": "Average Hourly Earnings",
        "description": "Avg Hourly Earnings of Production Employees (Monthly, SA)"
    },
    {
        "id": "FEDFUNDS",
        "name": "fed_funds_rate",
        "full_name": "Fed Funds Rate",
        "description": "Effective Federal Funds Rate (Monthly)"
    },
    {
        "id": "DGS2",
        "name": "2_year_treasury_yield",
        "full_name": "2-Year Treasury Yield",
        "description": "2-Year Treasury Note Yield (Daily)"
    },
    {
        "id": "DGS10",
        "name": "10_year_treasury_yield",
        "full_name": "10-Year Treasury Yield",
        "description": "10-Year Treasury Note Yield (Daily)"
    },
    {
        "id": "DGS30",
        "name": "30_year_treasury_yield",
        "full_name": "30-Year Treasury Yield",
        "description": "30-Year Treasury Bond Yield (Daily)"
    },
    {
        "id": "TB3MS",
        "name": "3_month_treasury_bill",
        "full_name": "3-Month Treasury Bill",
        "description": "3-Month Treasury Bill Secondary Market Rate (Monthly)"
    },
    {
        "id": "T10YIE",
        "name": "10_year_tips_spread",
        "full_name": "10-Year TIPS Spread",
        "description": "10-Year Breakeven Inflation Rate (Daily)"
    },
    {
        "id": "SP500",
        "name": "sp500_index",
        "full_name": "S&P 500 Index",
        "description": "S&P 500 Index (Daily)"
    },
    {
        "id": "VIXCLS",
        "name": "vix_volatility_index",
        "full_name": "VIX Volatility Index",
        "description": "CBOE Volatility Index (Daily)"
    },
    {
        "id": "UMCSENT",
        "name": "consumer_confidence",
        "full_name": "Consumer Confidence",
        "description": "University of Michigan Consumer Sentiment (Monthly)"
    },
    {
        "id": "RSXFS",
        "name": "retail_sales",
        "full_name": "Retail Sales",
        "description": "Advance Retail Sales (Monthly, SA)"
    },
    {
        "id": "INDPRO",
        "name": "industrial_production",
        "full_name": "Industrial Production",
        "description": "Industrial Production Index (Monthly, SA)"
    },
    {
        "id": "TCU",
        "name": "capacity_utilization",
        "full_name": "Capacity Utilization",
        "description": "Total Capacity Utilization (Monthly, SA)"
    },
    {
        "id": "IPMAN",
        "name": "manufacturing_production",
        "full_name": "Manufacturing Production",
        "description": "Industrial Production: Manufacturing (Monthly, SA)"
    },
    {
        "id": "HOUST",
        "name": "housing_starts",
        "full_name": "Housing Starts",
        "description": "Housing Starts (Monthly, SA)"
    },
    {
        "id": "PERMIT",
        "name": "building_permits",
        "full_name": "Building Permits",
        "description": "Building Permits (Monthly, SA)"
    },
    {
        "id": "HSN1F",
        "name": "new_home_sales",
        "full_name": "New Home Sales",
        "description": "New Single-Family Home Sales (Monthly, SA)"
    },
    {
        "id": "CSUSHPINSA",
        "name": "case_shiller_home_price",
        "full_name": "Case-Shiller Home Price",
        "description": "S&P/Case-Shiller U.S. National Home Price Index (Monthly, SA)"
    },
    {
        "id": "MORTGAGE30US",
        "name": "mortgage_rates_30_year",
        "full_name": "Mortgage Rates 30-Year",
        "description": "30-Year Fixed Mortgage Rate (Weekly)"
    },
    {
        "id": "PI",
        "name": "personal_income",
        "full_name": "Personal Income",
        "description": "Personal Income (Monthly, SA)"
    },
    {
        "id": "PCEC",
        "name": "personal_consumption",
        "full_name": "Personal Consumption",
        "description": "Personal Consumption Expenditures (Monthly, SA)"
    },
    {
        "id": "PSAVERT",
        "name": "personal_savings_rate",
        "full_name": "Personal Savings Rate",
        "description": "Personal Saving Rate (Monthly, SA)"
    },
    {
        "id": "DSPIC96",
        "name": "disposable_income",
        "full_name": "Disposable Income",
        "description": "Real Disposable Personal Income (Monthly, SA)"
    },
    {
        "id": "TOTALSL",
        "name": "consumer_credit",
        "full_name": "Consumer Credit",
        "description": "Total Consumer Credit Outstanding (Monthly, SA)"
    },
    {
        "id": "BOPGSTB",
        "name": "trade_balance",
        "full_name": "Trade Balance",
        "description": "Goods and Services Trade Balance (Monthly, SA)"
    },
    {
        "id": "EXPGS",
        "name": "exports",
        "full_name": "Exports",
        "description": "U.S. Exports of Goods and Services (Monthly, SA)"
    },
    {
        "id": "IMPGS",
        "name": "imports",
        "full_name": "Imports",
        "description": "U.S. Imports of Goods and Services (Monthly, SA)"
    },
    {
        "id": "NETFI",
        "name": "current_account_balance",
        "full_name": "Current Account Balance",
        "description": "Net International Investment Position (Quarterly)"
    },
    {
        "id": "M2SL",
        "name": "m2_money_supply",
        "full_name": "M2 Money Supply",
        "description": "M2 Money Stock (Monthly, SA)"
    },
    {
        "id": "M1SL",
        "name": "m1_money_supply",
        "full_name": "M1 Money Supply",
        "description": "M1 Money Stock (Monthly, SA)"
    },
    {
        "id": "M2V",
        "name": "velocity_of_m2",
        "full_name": "Velocity of M2",
        "description": "Velocity of M2 Money Stock (Quarterly)"
    },
    {
        "id": "TOTRESNS",
        "name": "bank_reserves",
        "full_name": "Bank Reserves",
        "description": "Total Reserves of Depository Institutions (Monthly, NSA)"
    },
    {
        "id": "TOTLL",
        "name": "commercial_bank_loans",
        "full_name": "Commercial Bank Loans",
        "description": "Total Loans and Leases at Commercial Banks (Monthly, SA)"
    },
    {
        "id": "AAA",
        "name": "corporate_bond_yield_aaa",
        "full_name": "Corporate Bond Yield AAA",
        "description": "Moody’s Seasoned Aaa Corporate Bond Yield (Monthly)"
    },
    {
        "id": "BAA",
        "name": "corporate_bond_yield_baa",
        "full_name": "Corporate Bond Yield BAA",
        "description": "Moody’s Seasoned Baa Corporate Bond Yield (Monthly)"
    },
    {
        "id": "BAMLH0A0HYM2",
        "name": "high_yield_spread",
        "full_name": "High Yield Spread",
        "description": "ICE BofA US High Yield Index Option-Adjusted Spread (Daily)"
    },
    {
        "id": "TEDRATE",
        "name": "ted_spread",
        "full_name": "TED Spread",
        "description": "TED Spread (Daily)"
    },
    {
        "id": "T10Y2Y",
        "name": "yield_curve_10y_2y",
        "full_name": "Yield Curve 10Y-2Y",
        "description": "10-Year minus 2-Year Treasury Yield Spread (Daily)"
    },
    {
        "id": "T10Y3M",
        "name": "yield_curve_10y_3m",
        "full_name": "Yield Curve 10Y-3M",
        "description": "10-Year minus 3-Month Treasury Yield Spread (Daily)"
    },
    {
        "id": "RRSFS",
        "name": "real_retail_sales",
        "full_name": "Real Retail Sales",
        "description": "Real Retail and Food Services Sales (Monthly, SA)"
    },
    {
        "id": "DGORDER",
        "name": "durable_goods_orders",
        "full_name": "Durable Goods Orders",
        "description": "Manufacturers’ New Orders: Durable Goods (Monthly, SA)"
    },
    {
        "id": "AMNMNO",
        "name": "nondurable_goods_orders",
        "full_name": "Nondurable Goods Orders",
        "description": "Manufacturers’ New Orders: Nondurable Goods (Monthly, SA)"
    },
    {
        "id": "BUSINV",
        "name": "business_inventories",
        "full_name": "Business Inventories",
        "description": "Total Business Inventories (Monthly, SA)"
    },
    {
        "id": "ISRATIO",
        "name": "inventory_to_sales_ratio",
        "full_name": "Inventory to Sales Ratio",
        "description": "Total Business Inventories to Sales Ratio (Monthly, SA)"
    },
    {
        "id": "USSLIND",
        "name": "leading_economic_index",
        "full_name": "Leading Economic Index",
        "description": "Conference Board Leading Economic Index (Monthly)"
    },
    {
        "id": "ICSA",
        "name": "initial_jobless_claims",
        "full_name": "Initial Jobless Claims",
        "description": "Initial Jobless Claims (Weekly, SA)"
    },
    {
        "id": "CCSA",
        "name": "continuing_claims",
        "full_name": "Continuing Claims",
        "description": "Continuing Jobless Claims (Weekly, SA)"
    },
    {
        "id": "OPHNFB",
        "name": "productivity",
        "full_name": "Productivity",
        "description": "Nonfarm Business Sector Productivity (Quarterly, SA)"
    },
    {
        "id": "ULCNFB",
        "name": "unit_labor_costs",
        "full_name": "Unit Labor Costs",
        "description": "Nonfarm Business Sector Unit Labor Costs (Quarterly, SA)"
    },
    {
        "id": "ECIWAG",
        "name": "employment_cost_index_wages_and_salaries",
        "full_name": "Employment Cost Index: Wages and Salaries",
        "description": "Employment Cost Index (Quarterly, SA)"
    },
    {
        "id": "TDSP",
        "name": "consumer_debt_service_ratio",
        "full_name": "Consumer Debt Service Ratio",
        "description": "Household Debt Service Payments as % of Disposable Income (Quarterly)"
    },
    {
        "id": "HDTGPDUSQ163N",
        "name": "household_debt_to_gdp",
        "full_name": "Household Debt to GDP",
        "description": "Household Debt to GDP Ratio (Quarterly)"
    },
    {
        "id": "COMREPUSQ159N",
        "name": "commercial_real_estate_prices",
        "full_name": "Commercial Real Estate Prices",
        "description": "Commercial Real Estate Price Index (Quarterly)"
    },
    {
        "id": "TTLCONS",
        "name": "construction_spending",
        "full_name": "Construction Spending",
        "description": "Total Construction Spending (Monthly, SA)"
    },
    {
        "id": "TOTALSA",
        "name": "vehicle_sales",
        "full_name": "Vehicle Sales",
        "description": "Total Vehicle Sales (Monthly, SA)"
    },
    {
        "id": "DCOILWTICO",
        "name": "oil_prices_wti",
        "full_name": "Oil Prices WTI",
        "description": "Crude Oil Prices: West Texas Intermediate (Daily)"
    },
    {
        "id": "WPU10210501",
        "name": "gold_ore_ppi",
        "full_name": "Gold Ore PPI",
        "description": "Producer Price Index for Gold Ores (Monthly, NSA)"
    },
    {
        "id": "PALLFNFINDEXQ",
        "name": "commodity_price_index",
        "full_name": "Commodity Price Index",
        "description": "Global Price Index of All Commodities (Quarterly)"
    },
    {
        "id": "CP",
        "name": "corporate_profits",
        "full_name": "Corporate Profits",
        "description": "Corporate Profits After Tax (Quarterly, SA)"
    },
    {
        "id": "GFDEGDQ188S",
        "name": "federal_debt_to_gdp",
        "full_name": "Federal Debt to GDP",
        "description": "Federal Debt as % of GDP (Quarterly)"
    },
    {
        "id": "MTSR133FMS",
        "name": "budget_deficit",
        "full_name": "Budget Deficit",
        "description": "Federal Surplus or Deficit (Monthly)"
    },
    {
        "id": "CUSR0000SAH1",
        "name": "consumer_price_index_shelter",
        "full_name": "Consumer Price Index Shelter",
        "description": "CPI: Shelter (Monthly, SA)"
    },
    {
        "id": "PCEC96",
        "name": "real_personal_consumption",
        "full_name": "Real Personal Consumption",
        "description": "Real Personal Consumption Expenditures (Monthly, SA)"
    },
    {
        "id": "IQ",
        "name": "export_price_index",
        "full_name": "Export Price Index",
        "description": "Export Price Index (Monthly, SA)"
    },
    {
        "id": "CONSUMER",
        "name": "consumer_loans",
        "full_name": "Consumer Loans",
        "description": "Consumer Loans at All Commercial Banks (Monthly, SA)"
    },
    {
        "id": "MPRIME",
        "name": "bank_prime_rate",
        "full_name": "Bank Prime Rate",
        "description": "Bank Prime Loan Rate (Monthly)"
    },
    {
        "id": "DRSFRMACBS",
        "name": "delinquency_rate_mortgages",
        "full_name": "Delinquency Rate Mortgages",
        "description": "Delinquency Rate on Single-Family Mortgages (Quarterly)"
    },
    {
        "id": "DRCLACBS",
        "name": "delinquency_rate_consumer_loans",
        "full_name": "Delinquency Rate Consumer Loans",
        "description": "Delinquency Rate on Consumer Loans (Quarterly)"
    },
    {
        "id": "USNIM",
        "name": "net_interest_margin",
        "full_name": "Net Interest Margin",
        "description": "Net Interest Margin for U.S. Banks (Quarterly)"
    },
    {
        "id": "TOTBKCR",
        "name": "bank_credit",
        "full_name": "Bank Credit",
        "description": "All Commercial Banks Credit (Monthly, SA)"
    },
    {
        "id": "FIXHAI",
        "name": "housing_affordability",
        "full_name": "Housing Affordability",
        "description": "Housing Affordability Index (Monthly)"
    },
    {
        "id": "RETAILIRSA",
        "name": "retail_inventories",
        "full_name": "Retail Inventories",
        "description": "Retail Inventories (Monthly, SA)"
    },
    {
        "id": "CFNAI",
        "name": "chicago_fed_national_activity",
        "full_name": "Chicago Fed National Activity",
        "description": "Chicago Fed National Activity Index (Monthly)"
    },
    {
        "id": "PENLISCOUUS",
        "name": "pending_listing_count",
        "full_name": "Pending Listing Count",
        "description": "Housing Inventory: Pending Listing Count in the United States (Monthly, SA)"
    }
]