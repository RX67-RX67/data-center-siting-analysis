SOURCES_COUNTY = {
    "transportation": {
        "path": "data/raw_data/transportation_2024/Table.csv",
        "format": "csv",
        "vintage": 2024,
        "read_dtypes": {
            "State": "string",
            "County Name": "string",
            # Features used in downstream computations / proxies
            "Large Primary Airports": "float64",
            "Medium Primary Airport": "float64",
            "Small Primary Airport": "float64",
            "Non-Hub Primary Airport": "float64",
            "National Non-Primary Airport": "float64",
            "Regional Non-Primary Airport": "float64",
            "Local Non-Primary Airport": "float64",
            "Basic Non-Primary Airport": "float64",
            "Unclassified Non-Primary Airport": "float64",
            "All Rail Track": "float64",
            "Docks": "float64",
            "Good": "float64",
            "Fair": "float64",
            "Poor": "float64",
        },
        "keys": {
            "state": "State",
            "county": "County Name",
        },
        "value_columns": {
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
        },
        "dtypes": {
            "state": "string",
            "county": "string",
            # Value columns used in proxy computations
            "primary_large_airport_count": "float64",
            "primary_medium_airport_count": "float64",
            "primary_small_airport_count": "float64",
            "non_hub_primary_airport_count": "float64",
            "national_non_primary_airport_count": "float64",
            "regional_non_primary_airport_count": "float64",
            "local_non_primary_airport_count": "float64",
            "basic_non_primary_airport_count": "float64",
            "unclassified_non_primary_airport_count": "float64",
            "rail_track_count": "float64",
            "docks_count": "float64",
            "infra_good_count": "float64",
            "infra_fair_count": "float64",
            "infra_poor_count": "float64",
        },
    },
    "environment_risk": {
        "path": "data/raw_data/environmental_risk/National_Risk_Index_Counties_807384124455672111.csv",
        "format": "csv",
        "vintage": 2025,
        "read_dtypes": {
            "State Name": "string",
            "County Name": "string",
            "County Type": "string",
            # Features used in downstream computations / proxies
            "Community Resilience - Value": "float64",
            "Community Risk Factor - Value": "float64",
            "Cold Wave - Hazard Type Risk Index Value": "float64",
            "Drought - Hazard Type Risk Index Value": "float64",
            "Earthquake - Hazard Type Risk Index Value": "float64",
            "Hail - Hazard Type Risk Index Value": "float64",
            "Heat Wave - Hazard Type Risk Index Value": "float64",
            "Hurricane - Hazard Type Risk Index Value": "float64",
            "Ice Storm - Hazard Type Risk Index Value": "float64",
            "Landslide - Hazard Type Risk Index Value": "float64",
            "Lightning - Hazard Type Risk Index Value": "float64",
            "Riverine Flooding - Hazard Type Risk Index Value": "float64",
            "Strong Wind - Hazard Type Risk Index Value": "float64",
            "Tornado - Hazard Type Risk Index Value": "float64",
            "Wildfire - Hazard Type Risk Index Value": "float64",
            "Winter Weather - Hazard Type Risk Index Value": "float64",
        },
        "keys": {
            "state": "State Name",
            "county": "county_name",
        },
        "combine_columns": {
            "county_name": {
                "from": ["County Name", "County Type"],
                "method": "concat",
                "separator": " ",
                "dtype": "string",
            }
        },
        "value_columns": {
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
            "winter_weather_risk_index_value": "Winter Weather - Hazard Type Risk Index Value",
        },
        "dtypes": {
            "state": "string",
            "county": "string",  # constructed from County Name + County Type
            # Value columns used in downstream computations
            "community_resilience_value": "float64",
            "community_risk_factor_value": "float64",
            "cold_wave_risk_index_value": "float64",
            "drought_risk_index_value": "float64",
            "earthquake_risk_index_value": "float64",
            "hail_risk_index_value": "float64",
            "heat_wave_risk_index_value": "float64",
            "hurricane_risk_index_value": "float64",
            "ice_storm_risk_index_value": "float64",
            "landslide_risk_index_value": "float64",
            "lightning_risk_index_value": "float64",
            "riverine_flooding_risk_index_value": "float64",
            "strong_wind_risk_index_value": "float64",
            "tornado_risk_index_value": "float64",
            "wildfire_risk_index_value": "float64",
            "winter_weather_risk_index_value": "float64",
        },
    },
    "labor_price": {
        "path": "data/raw_data/labor_cost_2023/allhlcn23.xlsx",
        "format": "xlsx",
        "vintage": 2023,
        "sheet": "US_St_Cn_MSA",
        "read_dtypes": {
            "St Name": "string",
            "Area": "string",
            "Area Type": "string",
            "Ownership": "string",
            "Industry": "string",
            # Feature used as the wage value for pivot / aggregations
            "Annual Average Weekly Wage": "float64",
        },
        "keys": {
            "state": "St Name",
            "county": "Area",
        },
        "normalize": {
            "county": {
                "method": "remove anything after the word 'County'",
            }
        },
        "filter": {
            "Area Type": "County",
            "Ownership": "Private",
            "Industry": ["1021 Trade, transportation, and utilities", "1022 Information", "1024 Professional and business services"],
        },
        "pivot": {
            "index": ["St Name", "Area"],
            "columns": "Industry",
            "values": "Annual Average Weekly Wage",
            "rename": {
                "1021 Trade, transportation, and utilities": "wage_trade_transport_utilities",
                "1022 Information": "wage_information",
                "1024 Professional and business services": "wage_prof_business",
            }
        },
        "dtypes": {
            "state": "string",
            "county": "string",
            "wage_trade_transport_utilities": "float64",
            "wage_information": "float64",
            "wage_prof_business": "float64",
        },
    },
}