"""
Unified source configuration for all reference, county, and ZIP tables.

Canonical keys (aligned across tables):
- county_fips: 5-digit FIPS code (state 2 + county 3)
- zip_code: 5-digit ZIP code
- state: 2-letter state code
- county_name: County name (for tables without county_fips; join via fips_to_county to resolve)

Grain: zip | county_fips | county_state_name
- zip: join key = zip_code
- county_fips: join key = county_fips (or state + county_name)
- county_state_name: join key = state + county_name (builder adds county_fips via reference)
"""

SOURCES = {
    # ---- Reference (ZIP↔county mapping) ----
    "zip_to_fips": {
        "grain": "zip",
        "path": "data/raw_data/zip_county_transformation/ZIP_COUNTY_092025.xlsx",
        "format": "xlsx",
        "vintage": 2025,
        "sheet": "Export Worksheet",
        "keys": {
            "county_fips": "COUNTY",
            "zip_code": "ZIP",
        },
        "value_columns": {
            "state_cap": "USPS_ZIP_PRE_STATE",
            "res_ratio": "RES_RATIO",
            "business_ratio": "BUS_RATIO",
            "other_ratio": "OTH_RATIO",
            "total_ratio": "TOT_RATIO",
        },
    },
    "fips_to_county": {
        "grain": "county_fips",
        "path": "data/raw_data/zip_county_transformation/all-geocodes-v2024.xlsx",
        "format": "xlsx",
        "vintage": 2024,
        "sheet": "all_geocodes_v2024",
        "skiprows": 4,
        "filters": {"Summary Level": "050"},
        "keys": {"county_fips": "County FIPS Code"},
        "combine_columns": {
            "county_fips": {
                "from": ["State FIPS Code", "County FIPS Code"],
                "method": "concat_zfill",
                "zfill": [2, 3],
                "dtype": "string",
            }
        },
        "post_filters": {"county_fips_not_ending_with": "000"},
        "value_columns": {"county_name": "Area Name"},
    },
    # ---- County (state + county_name; builder adds county_fips via fips_to_county) ----
    "transportation": {
        "grain": "county_state_name",
        "path": "data/raw_data/transportation_2024/Table.csv",
        "format": "csv",
        "vintage": 2024,
        "keys": {"state": "State", "county_name": "County Name"},
        "value_columns": {
            "primary_large_airport_count": "Large Primary Airports",
            "primary_medium_airport_count": "Medium Primary Airports",
            "primary_small_airport_count": "Small Primary Airports",
            "non_hub_primary_airport_count": "Non-Hub Primary Airport",
            "national_non_primary_airport_count": "National Non-Primary Airport",
            "regional_non_primary_airport_count": "Regional Non-Primary Airport",
            "local_non_primary_airport_count": "Local Non-Primary Airport",
            "basic_non_primary_airport_count": "Basic Non-Primary Airport",
            "unclassified_non_primary_airport_count": "Unclassified Non-Primary Airport",
            "rail_track_count": "All Rail Track",
            "docks_count": "Docks",
            "infra_good_count": "Good",
            "infra_fair_count": "Fair",
            "infra_poor_count": "Poor",
        },
        "proxies": {
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
            "infrastructure_quality": {
                "comment": (
                    "Weighted index of infrastructure condition: "
                    "(Good + 0.5*Fair) / (Good + Fair + Poor)."
                )
            },
            "dock_presence": {
                "comment": (
                    "Binary indicator of whether the county has at least one dock or port facility. "
                    "Used to capture structural access to water-based freight and industrial logistics. "
                    "Zeros are treated as structural zeros (e.g., landlocked counties), not missing data."
                )
            },
        },
    },
    "environment_risk": {
        "grain": "county_state_name",
        "path": "data/raw_data/environmental_risk/National_Risk_Index_Counties_807384124455672111.csv",
        "format": "csv",
        "vintage": 2025,
        "keys": {"state": "State Name", "county_name": "County Name"},
        "value_columns": {
            "community_resilience_value": "Community Resilience - Value",
            "community_risk_factor_value": "Community Risk Factor - Value",
            "cold_wave_risk_index_value": "Cold Wave - Hazard Type Risk Index Value",
            "drought_risk_index_value": "Drought - Hazard Type Risk Index Value",
            "earthquake_risk_index_value": "Earthquake - Hazard Type Risk Index Value",
            "hail_risk_index_value": "Hail - Hazard Type Risk Index Value",
            "heat_wave_risk_index_value": "Heat Wave - Hazard Type Risk Index Value",
            "hurricane_risk_index_value": "Hurricane - Hazard Type Risk Index Value",
            "ice_storm_risk_index_value": "Ice Storm - Hazard Type Risk Index Value",
            "landslide_risk_index_value": "Landslide - Hazard Type Risk Index Value",
            "lightning_risk_index_value": "Lightning - Hazard Type Risk Index Value",
            "riverine_flooding_risk_index_value": "Riverine Flooding - Hazard Type Risk Index Value",
            "strong_wind_risk_index_value": "Strong Wind - Hazard Type Risk Index Value",
            "tornado_risk_index_value": "Tornado - Hazard Type Risk Index Value",
            "wildfire_risk_index_value": "Wildfire - Hazard Type Risk Index Value",
            "winter_weather_risk_index_value": "Winter Weather - Hazard Type Risk Index Value",
        },
    },
    "land_price": {
        "grain": "county_state_name",
        "path": "data/raw_data/land_price_2023/AEI_adjusted-Land-Data-2023.xlsx",
        "format": "xlsx",
        "vintage": 2023,
        "sheet": "Census Tract",
        "keys": {"state": "State", "county_name": "County"},
        "normalize": {
            "county_name": {
                "method": "remove anything after the county name including the word 'county'",
            }
        },
        "filter": {"Year": 2023},
        "aggregation": {
            "method": "median",
            "groupby": ["state", "county_name"],
            "value_columns": {
                "land_value_1_4_acre_standardized": "Land Value (1/4 Acre Lot, Standardized)"
            },
        },
    },
    "labor_price": {
        "grain": "county_state_name",
        "path": "data/raw_data/labor_cost_2023/allhlcn23.xlsx",
        "format": "xlsx",
        "vintage": 2023,
        "sheet": "US_St_CN_MSA",
        "keys": {"state": "St Name", "county_name": "Area"},
        "normalize": {
            "county_name": {
                "method": "remove anything after the county name including the word 'county'",
            }
        },
        "filter": {
            "Area Type": "County",
            "Onwership": "Private",
            "Industry": [
                "1021 Trade, transportation, and utilities",
                "1022 Information",
                "1024 Professional and business services",
            ],
        },
        "pivot": {
            "index": ["St Name", "Area"],
            "columns": "Industry",
            "values": "Annual Average Weekly Wage",
            "rename": {
                "1021 Trade, transportation, and utilities": "wage_trade_transport_utilities",
                "1022 Information": "wage_information",
                "1024 Professional and business services": "wage_prof_business",
            },
        },
    },
    # ---- County (county_fips available) ----
    "grid_infrastructure": {
        "grain": "county_fips",
        "path": "data/raw_data/grid_2023/2023 USEER County Data_1.xlsx",
        "format": "xlsx",
        "vintage": 2023,
        "sheet": "Sheet1",
        "skiprows": 6,
        "keys": {
            "state": "State",
            "county_fips": "County FIPS",
            "county_name": "County Name",
        },
        "special_values": {
            "<10": {
                "meaning": "fewer than 10 jobs, suppressed for privacy",
                "replace_with": 5,
            }
        },
        "value_columns": {
            "epg_solar": "Solar",
            "epg_wind": "Wind",
            "epg_hydroelectric": "Hydroelectric",
            "epg_natural_gas": "Natural Gas",
            "tds_traditional": "Traditional TDS",
            "tds_storage": "Storage",
            "tds_smart_grid": "Smart Grid",
            "tds_micro_grid": "Micro Grid",
        },
        "proxies": {
            "clean_energy_jobs": {
                "comment": (
                    "Sum of Solar + Wind + Hydroelectric. Proxies renewable power presence "
                    "and clean-energy workforce; relevant for DC siting (RE targets, PPA availability)."
                ),
                "columns": ["epg_solar", "epg_wind", "epg_hydroelectric"],
            },
            "grid_infrastructure_jobs": {
                "comment": (
                    "Sum of Traditional TDS + Storage + Smart Grid + Micro Grid. Proxies grid "
                    "maturity and workforce for grid ops; signals reliability and upgrade capacity."
                ),
                "columns": ["tds_traditional", "tds_storage", "tds_smart_grid", "tds_micro_grid"],
            },
        },
    },
    "high_speed_internet": {
        "grain": "county_fips",
        "path": "data/raw_data/Internet_2025/bdc_us_fixed_broadband_summary_by_geography_D23_01may2025 2.csv",
        "format": "csv",
        "vintage": 2023,
        "filter": {
            "geography_type": "County",
            "biz_res": "B",
            "technology": ["Fiber", "Cable/Fiber", "Any Technology"],
        },
        "keys": {"county_fips": "geography_id"},
        "pivot": {
            "index": ["geography_id"],
            "columns": "technology",
            "values": ["speed_100_20", "speed_1000_100"],
            "flatten_names": True,
        },
        "value_columns": {
            "fiber_100_20_coverage": "Fiber_speed_100_20",
            "fiber_1000_100_coverage": "Fiber_speed_1000_100",
            "cable_fiber_100_20_coverage": "Cable/Fiber_speed_100_20",
            "cable_fiber_1000_100_coverage": "Cable/Fiber_speed_1000_100",
            "any_tech_100_20_coverage": "Any Technology_speed_100_20",
            "any_tech_1000_100_coverage": "Any Technology_speed_1000_100",
        },
        "proxies": {
            "fiber_availability": {
                "comment": (
                    "Fiber coverage at 1 Gbps+ (fiber_1000_100_coverage). "
                    "Critical for data centers: fiber provides low latency, high bandwidth, and redundancy."
                ),
                "column": "fiber_1000_100_coverage",
            },
            "high_speed_wired_coverage": {
                "comment": (
                    "Cable/Fiber coverage at 1 Gbps+ (cable_fiber_1000_100_coverage). "
                    "Wired infrastructure signal for high-bandwidth connectivity."
                ),
                "column": "cable_fiber_1000_100_coverage",
            },
        },
    },
    # ---- ZIP ----
    "electricity_price": {
        "grain": "zip",
        "vintage": 2023,
        "sources": [
            {"path": "data/raw_data/electricity_price_2023/iou_zipcodes_2023.csv", "format": "csv"},
            {"path": "data/raw_data/electricity_price_2023/non_iou_zipcodes_2023.csv", "format": "csv"},
        ],
        "concat": True,
        "keys": {"zip_code": "zip"},
        "value_columns": {
            "ownership": "ownership",
            "commercial_price": "comm_rate",
            "industrial_price": "ind_rate",
            "residential_price": "res_rate",
        },
        "aggregation": {
            "comment": (
                "Multiple utilities may serve the same ZIP code. "
                "Aggregate by taking the mean of each rate column per ZIP."
            ),
            "method": "mean",
            "groupby": ["zip_code"],
        },
    },
}
