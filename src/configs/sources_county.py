SOURCES_COUNTY = {
    "transportation": {
        "path": "data/raw_data/transportation_2024/Table.csv",
        "format": "csv",
        "vintage": 2024,
        # common keys for tables merging
        "keys":{
            "state": "State",
            "county": "County Name",
        },
        # value columns for the table(with name transform)
        "value_columns":{
            "primary_large_airport_count":"Large Primary Airports",
            "primary_medium_airport_count":"Medium Primary Airports",
            "primary_small_airport_count":"Small Primary Airports",
            "non_hub_primary_airport_count":"Non-Hub Primary Airport",
            "national_non_primary_airport_count":"National Non-Primary Airport",
            "regional_non_primary_airport_count":"Regional Non-Primary Airport",
            "local_non_primary_airport_count":"Local Non-Primary Airport",
            "basic_non_primary_airport_count":"Basic Non-Primary Airport",
            "unclassified_non_primary_airport_count":"Unclassified Non-Primary Airport",
            "rail_track_count":"All Rail Track",
            "docks_count":"Docks",
            "infra_good_count":"Good",
            "infra_fair_count":"Fair",
            "infra_poor_count":"Poor"
        },
        # proxies to be built based on the value columns
        "proxies":{
            "air_connectivity": {
                "comment": (
                    "Weighted index of airport availability and size. "
                    "Weights reflect FAA airport hierarchy: "
                    "Large(5), Medium(4), Small(3), Non-Hub Primary(2), "
                    "National(1.5), Regional(1.0), Local(0.7), Basic(0.4), Unclassified(0.2). "
                    "Final feature is log1p-transformed."
                )
            },
            "rail_intensity": {
                "comment": (
                    "Log-transformed all rail track count using log1p to reduce right skewness "
                    "and mitigate dominance of extreme values, while preserving structural zeros."
                )
            },
            "infrastructure_quality":{
                "comment" : (
                    "Weighted index of infrastructure condition: "
                    "(Good + 0.5*Fair) / (Good + Fair + Poor)."
                )
            },
            "dock_presence":{
                "comment" :(
                    "Binary indicator of whether the county has at least one dock or port facility. "
                    "Used to capture structural access to water-based freight and industrial logistics. "
                    "Zeros are treated as structural zeros (e.g., landlocked counties), not missing data."
                )
            }
        }
    },
    "environment_risk": {
        "path": "data/raw_data/environmental_risk/National_Risk_Index_Counties_807384124455672111.csv",
        "format": "csv",
        "vintage": 2025,
        "keys":{
            "state": "State Name",
            "county": "county_name",
        },
         "combine_columns": {
            "county_name": {
                "from": ["County Name", "County Type"],
                "method": "concat",
                "separator": " ",
                "dtype": "string"
            }
        },
        "value_columns":{
            "community_resilience_value":"Community Resilience - Value",
            "community_risk_factor_value":"Community Risk Factor - Value",
            "cold_wave_risk_index_value":"Cold Wave - Hazard Type Risk Index Value",
            "drought_risk_index_value":"Drought - Hazard Type Risk Index Value",
            "earthquake_risk_index_value":"Earthquake - Hazard Type Risk Index Value",
            "hail_risk_index_value":"Hail - Hazard Type Risk Index Value",
            "heat_wave_risk_index_value":"Heat Wave - Hazard Type Risk Index Value",
            "hurricane_risk_index_value":"Hurricane - Hazard Type Risk Index Value",
            "ice_storm_risk_index_value":"Ice Storm - Hazard Type Risk Index Value",
            "landslide_risk_index_value":"Landslide - Hazard Type Risk Index Value",
            "lightning_risk_index_value":"Lightning - Hazard Type Risk Index Value",
            "riverine_flooding_risk_index_value":"Riverine Flooding - Hazard Type Risk Index Value",
            "strong_wind_risk_index_value":"Strong Wind - Hazard Type Risk Index Value",
            "tornado_risk_index_value":"Tornado - Hazard Type Risk Index Value",
            "wildfire_risk_index_value":"Wildfire - Hazard Type Risk Index Value",
            "winter_weather_risk_index_value":"Winter Weather - Hazard Type Risk Index Value"
        }
    },
    "labor_price": {
        "path": "data/raw_data/labor_cost_2023/allhlcn23.xlsx",
        "format": "xlsx",
        "vintage": 2023,
        "sheet": "US_St_Cn_MSA",
        "keys": {
            "state": "St Name",
            "county": "Area",
        },
        "normalize":{
            "county":{
                "method": "remove anything after the county name including the word 'county'",
            }
        },
        "filter":{
            "Area Type": "County",
            "Ownership": "Private",
            "Industry":["1021 Trade, transportation, and utilities", "1022 Information", "1024 Professional and business services"]
        },
        "pivot": {
            "index": ["St Name", "Area"],
            "columns": "Industry",
            "values": "Annual Average Weekly Wage",
            "rename": {
            "1021 Trade, transportation, and utilities": "wage_trade_transport_utilities",
            "1022 Information": "wage_information",
            "1024 Professional and business services": "wage_prof_business"
            }
        } 
    }
}