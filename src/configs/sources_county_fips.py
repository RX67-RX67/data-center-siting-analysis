SOURCES_COUNTY_FIPS = {
    "grid_infrastructure": {
        "path": "data/raw_data/grid_2023/2023 USEER County Data_1.xlsx",
        "format": "xlsx",
        "vintage": 2023,  # USEER 2023 report (covers 2022 employment data)
        "sheet": "Sheet1",
        "skiprows": 6,  # Header row is row 7 (0-indexed: 6)
        "read_dtypes": {
            "County FIPS": "int64",
            "State": "string",
            "County Name": "string",
            # Features used in downstream computations / proxies
            "Solar": "string",  # Contains "<10" special values, will be processed
            "Wind": "string",
            "Hydroelectric": "string",
            "Natural Gas": "string",
            "Traditional TDS": "string",
            "Storage": "string",
            "Smart Grid": "string",
            "Micro Grid": "string",
        },
        "keys": {
            "county_fips": "County FIPS",
        },
        # Special value handling: "<10" means fewer than 10 jobs (privacy protection)
        "special_values": {
            "<10": {
                "meaning": "fewer than 10 jobs, suppressed for privacy",
                "replace_with": 5,  # midpoint imputation
            }
        },
        # Trimmed for data-center siting: power supply, clean energy, grid infrastructure.
        # Dropped: fuels, EE, motor vehicles, coal/oil/other generation, vague columns.
        "value_columns": {
            # County name and state; for the purpose of joining with county-level tables
            "state": "State",
            "county": "County Name",
            # Electric power generation — renewables + gas backbone (relevant for power supply)
            "epg_solar": "Solar",
            "epg_wind": "Wind",
            "epg_hydroelectric": "Hydroelectric",
            "epg_natural_gas": "Natural Gas",
            # Transmission, distribution & storage — grid capacity and workforce
            "tds_traditional": "Traditional TDS",
            "tds_storage": "Storage",
            "tds_smart_grid": "Smart Grid",
            "tds_micro_grid": "Micro Grid",
        },
        "proxies": {
            "clean_energy_jobs": {
                "comment": (
                    "Sum of Solar + Wind + Hydroelectric. Proxies renewable power presence and "
                    "clean-energy workforce; relevant for DC siting (RE targets, PPA availability)."
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
        "dtypes": {
            "county_fips": "string",
            "state": "string",
            "county": "string",
            # Value columns used in proxy computations
            "epg_solar": "float64",
            "epg_wind": "float64",
            "epg_hydroelectric": "float64",
            "epg_natural_gas": "float64",
            "tds_traditional": "float64",
            "tds_storage": "float64",
            "tds_smart_grid": "float64",
            "tds_micro_grid": "float64",
        },
    },
    "high_speed_internet": {
        "path": "data/raw_data/Internet_2025/bdc_us_fixed_broadband_summary_by_geography_D23_01may2025 2.csv",
        "format": "csv",
        "vintage": 2023,  # D23 = December 2023 data collection, released May 2025
        "read_dtypes": {
            "geography_id": "string",
            # Features used in filtering
            "geography_type": "string",
            "biz_res": "string",
            "technology": "string",
            # Features used in pivot / aggregations
            "speed_100_20": "float64",
            "speed_1000_100": "float64",
        },
        "filter": {
            "geography_type": "County",
            "biz_res": "B",  # Business broadband (data centers are business locations)
            "technology": ["Fiber", "Cable/Fiber", "Any Technology"],
        },
        "keys": {
            "county_fips": "geography_id",
        },
        # Pivot: technology as columns, speed columns as values
        "pivot": {
            "index": ["geography_id"],
            "columns": "technology",
            "values": ["speed_100_20", "speed_1000_100"],
            "flatten_names": True,
        },
        "value_columns": {
            # Fiber availability (best for data centers)
            "fiber_100_20_coverage": "Fiber_speed_100_20",  # % business locations with fiber ≥100/20
            "fiber_1000_100_coverage": "Fiber_speed_1000_100",  # % with fiber ≥1000/100 (1 Gbps)
            # Cable/Fiber combined (wired high-speed)
            "cable_fiber_100_20_coverage": "Cable/Fiber_speed_100_20",
            "cable_fiber_1000_100_coverage": "Cable/Fiber_speed_1000_100",
            # Overall (any technology)
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
        "dtypes": {
            "county_fips": "string",
            # Value columns from pivot operation
            "fiber_100_20_coverage": "float64",
            "fiber_1000_100_coverage": "float64",
            "cable_fiber_100_20_coverage": "float64",
            "cable_fiber_1000_100_coverage": "float64",
            "any_tech_100_20_coverage": "float64",
            "any_tech_1000_100_coverage": "float64",
        },
    },
    "land_price": {
        "path": "data/raw_data/land_price_2023/AEI_adjusted-Land-Data-2023.xlsx",
        "format": "xlsx",
        "vintage": 2023,
        "sheet": "County",
        "read_dtypes": {
            "County Code": "int64",
            # Feature used in filtering
            "Year": "int64",
            # Feature used in downstream computations
            "Land Value (1/4 Acre Lot, Standardized)": "float64",
        },
        "keys": {
            "county_fips": "County Code",
        },
        # filter for the table
        "filter":{
            "Year": 2023,
        },
        "value_columns": {
            "land_value_1_4_acre_standardized": "Land Value (1/4 Acre Lot, Standardized)"
        },
        "dtypes": {
            "county_fips": "string",
            "land_value_1_4_acre_standardized": "float64",
        },
    }
}