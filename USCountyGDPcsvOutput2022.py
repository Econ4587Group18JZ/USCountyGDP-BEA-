import pandas as pd
import requests
from typing import Optional

def get_county_gdp_by_state(api_key: str, year: str = "2022") -> pd.DataFrame:
    """
    Fetch GDP for all counties and organize by state
    """
    
    # API parameters
    params = {
        "UserID": api_key,
        "method": "GetData",
        "datasetname": "Regional",
        "TableName": "CAGDP9",  # Real GDP (chained 2017 dollars)
        "LineCode": 1,          # All industry total
        "GeoFips": "COUNTY",    # All counties
        "Year": year,
        "ResultFormat": "JSON"
    }
    
    try:
        # Make API request
        response = requests.get("https://apps.bea.gov/api/data", params=params)
        data = response.json()
        
        if "BEAAPI" in data and "Results" in data["BEAAPI"]:
            results = data["BEAAPI"]["Results"]
            if "Data" in results:
                # Convert to DataFrame
                df = pd.DataFrame(results["Data"])
                
                # Clean up the data
                df["GDP"] = pd.to_numeric(df["DataValue"].str.replace(',', ''), errors='coerce')
                df["State"] = df["GeoName"].str.extract(r', (\w{2})$')
                df["County"] = df["GeoName"].str.replace(r' County, \w{2}$', '', regex=True)
                df["GDP_Billions"] = df["GDP"] / 1_000_000
                
                # Get state names mapping
                state_names = {
                    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 
                    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
                    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 
                    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
                    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 
                    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
                    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 
                    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
                    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
                    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
                    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
                    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
                    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
                }
                
                # Add full state name
                df['StateName'] = df['State'].map(state_names)
                
                # Sort by state name and then by GDP within each state
                df_sorted = df.sort_values(['StateName', 'GDP'], ascending=[True, False])
                
                # Create clean output
                output_df = df_sorted[[
                    'StateName', 'County', 'GDP_Billions', 'GeoFips'
                ]].copy()
                
                # Round GDP to 3 decimal places
                output_df['GDP_Billions'] = output_df['GDP_Billions'].round(3)
                
                return output_df
                
        return pd.DataFrame()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def print_gdp_by_state(df: pd.DataFrame):
    """
    Print GDP data organized by state with formatting
    """
    current_state = None
    state_rank = 0
    
    # Calculate total US GDP
    total_us_gdp = df['GDP_Billions'].sum()
    print(f"Total US GDP (Billions): ${total_us_gdp:,.3f}")
    print("\nGDP by State and County (2022)")
    print("=" * 80)
    
    for _, row in df.iterrows():
        # When we encounter a new state
        if current_state != row['StateName']:
            current_state = row['StateName']
            state_rank = 1
            
            # Calculate state total GDP
            state_total = df[df['StateName'] == current_state]['GDP_Billions'].sum()
            
            # Print state header
            print(f"\n{current_state}")
            print(f"State Total GDP: ${state_total:,.3f} Billion")
            print("-" * 80)
            print(f"{'Rank':>4} {'County':<30} {'GDP (Billions)':>15} {'FIPS':>10}")
            print("-" * 80)
        
        # Print county data
        print(f"{state_rank:4d} {row['County']:<30} ${row['GDP_Billions']:14,.3f} {row['GeoFips']:>10}")
        state_rank += 1

# Fetch and display the data
api_key = "7A906245-8730-46D1-B255-251FA0FB0247"
county_gdp = get_county_gdp_by_state(api_key)

if not county_gdp.empty:
    print_gdp_by_state(county_gdp)
    
    # Save to CSV
    csv_filename = "county_gdp_by_state_2022.csv"
    county_gdp.to_csv(csv_filename, index=False)
    print(f"\nData saved to {csv_filename}")
