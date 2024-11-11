import pandas as pd
import sqlite3
import requests
import time
from datetime import datetime

def fetch_county_gdp(api_key, year):
    """Fetch county GDP data from BEA API for a specific year"""
    
    base_url = "https://apps.bea.gov/api/data/"
    params = {
        "UserID": api_key,
        "method": "GetData",
        "datasetname": "Regional",
        "TableName": "CAGDP9",  # Real GDP by county (chained 2017 dollars)
        "LineCode": 1,          # All industry total
        "GeoFips": "COUNTY",   # Get all counties
        "Year": str(year),
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if "BEAAPI" in data and "Results" in data["BEAAPI"]:
            results = data["BEAAPI"]["Results"]
            if "Data" in results:
                df = pd.DataFrame(results["Data"])
                print(f"Successfully fetched {year} data: {len(df)} records")
                return df
            
        print(f"No data found for {year}")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {year}: {e}")
        return None

def process_bea_data(df):
    """Process raw BEA API data into clean format"""
    if df is None or df.empty:
        return None
        
    # Clean up the data
    df['gdp_billions'] = pd.to_numeric(df['DataValue'].str.replace(',', ''), errors='coerce') / 1000
    
    # Extract state and county from GeoName
    df[['county_name', 'state_code']] = df['GeoName'].str.extract(r'(.*), (\w{2})$')
    
    # Clean up FIPS codes
    df['fips_code'] = df['GeoFips']
    
    # Add year
    df['year'] = pd.to_numeric(df['TimePeriod'])
    
    return df

def create_database(all_data):
    """Create SQLite database with county GDP data"""
    
    db_name = 'county_gdp_historical.db'
    conn = sqlite3.connect(db_name)
    
    # Create tables
    conn.execute('''
        CREATE TABLE IF NOT EXISTS states (
            state_code TEXT,
            total_gdp REAL,
            num_counties INTEGER,
            year INTEGER,
            PRIMARY KEY (state_code, year)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS counties (
            fips_code TEXT,
            county_name TEXT,
            state_code TEXT,
            gdp_billions REAL,
            year INTEGER,
            PRIMARY KEY (fips_code, year),
            FOREIGN KEY (state_code, year) REFERENCES states(state_code, year)
        )
    ''')
    
    # Calculate state summaries
    state_summaries = all_data.groupby(['state_code', 'year']).agg({
        'gdp_billions': ['sum', 'count']
    }).reset_index()
    
    state_summaries.columns = ['state_code', 'year', 'total_gdp', 'num_counties']
    
    # Insert state data
    state_summaries.to_sql('states', conn, if_exists='replace', index=False)
    
    # Prepare and insert county data
    county_data = all_data[['fips_code', 'county_name', 'state_code', 'gdp_billions', 'year']]
    county_data.to_sql('counties', conn, if_exists='replace', index=False)
    
    # Create views
    conn.execute('''
        CREATE VIEW IF NOT EXISTS county_growth AS
        SELECT 
            c1.fips_code,
            c1.county_name,
            c1.state_code,
            c1.year,
            c1.gdp_billions,
            c1.gdp_billions / c2.gdp_billions - 1 as annual_growth_rate
        FROM counties c1
        LEFT JOIN counties c2 
            ON c1.fips_code = c2.fips_code 
            AND c1.year = c2.year + 1
    ''')
    
    conn.execute('''
        CREATE VIEW IF NOT EXISTS state_growth AS
        SELECT 
            s1.state_code,
            s1.year,
            s1.total_gdp,
            s1.total_gdp / s2.total_gdp - 1 as annual_growth_rate
        FROM states s1
        LEFT JOIN states s2 
            ON s1.state_code = s2.state_code 
            AND s1.year = s2.year + 1
    ''')
    
    conn.execute('''
        CREATE VIEW IF NOT EXISTS county_rankings_by_year AS
        SELECT 
            c.state_code,
            c.county_name,
            c.gdp_billions,
            c.year,
            RANK() OVER (PARTITION BY c.state_code, c.year ORDER BY c.gdp_billions DESC) as rank_in_state,
            RANK() OVER (PARTITION BY c.year ORDER BY c.gdp_billions DESC) as rank_national
        FROM counties c
    ''')
    
    # Create indexes
    conn.execute('CREATE INDEX IF NOT EXISTS idx_counties_state_year ON counties(state_code, year)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_counties_gdp ON counties(gdp_billions)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_counties_fips_year ON counties(fips_code, year)')
    
    # Commit changes
    conn.commit()
    return conn

def main():
    # BEA API key
    API_KEY = "7A906245-8730-46D1-B255-251FA0FB0247"
    
    # Years to fetch (2004-2022)
    years = range(2004, 2023)
    
    # Store all data
    all_data_frames = []
    
    # Fetch data for each year
    print("Fetching historical data from BEA API...")
    for year in years:
        print(f"\nFetching {year} data...")
        raw_data = fetch_county_gdp(API_KEY, year)
        if raw_data is not None:
            processed_data = process_bea_data(raw_data)
            if processed_data is not None:
                all_data_frames.append(processed_data)
        # Add delay to respect API rate limits
        time.sleep(0.5)
    
    if not all_data_frames:
        print("No data was successfully fetched")
        return
        
    # Combine all years
    print("\nCombining all years...")
    combined_data = pd.concat(all_data_frames, ignore_index=True)
    
    # Create database
    print("Creating database...")
    conn = create_database(combined_data)
    
    # Verify the data
    cursor = conn.cursor()
    
    # Check counts by year
    cursor.execute('''
        SELECT year, COUNT(DISTINCT state_code) as num_states, COUNT(*) as num_counties
        FROM counties
        GROUP BY year
        ORDER BY year
    ''')
    
    print("\nData coverage by year:")
    for year, num_states, num_counties in cursor.fetchall():
        print(f"{year}: {num_states} states, {num_counties} counties")
    
    # Example historical analysis
    print("\nExample growth analysis:")
    cursor.execute('''
        SELECT year, 
               AVG(annual_growth_rate) * 100 as avg_growth,
               MIN(annual_growth_rate) * 100 as min_growth,
               MAX(annual_growth_rate) * 100 as max_growth
        FROM county_growth
        WHERE annual_growth_rate IS NOT NULL
        GROUP BY year
        ORDER BY year DESC
        LIMIT 5
    ''')
    
    print("\nRecent county GDP growth rates:")
    print("Year  | Avg Growth % | Min Growth % | Max Growth %")
    print("-" * 50)
    for row in cursor.fetchall():
        print(f"{row[0]}  | {row[1]:10.2f} | {row[2]:10.2f} | {row[3]:10.2f}")
    
    conn.close()
    print("\nDatabase creation complete.")
    print("Database file: county_gdp_historical.db")
    print("Tables: states, counties")
    print("Views: county_growth, state_growth, county_rankings_by_year")

if __name__ == "__main__":
    main()
