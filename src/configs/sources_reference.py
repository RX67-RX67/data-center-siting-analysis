SOURCES_REFERENCE = {
    "zip_to_fips": {
        "path": "data/raw_data/zip_county_transformation/ZIP_COUNTY_092025.xlsx",
        "format": "xlsx",
        "vintage": 2025,  # September 2025
        "sheet": "Export Worksheet",
        # Apply at read time so no later step can alter or lose values (e.g. leading zeros)
        "read_dtypes": {
            "ZIP": "string",
            "COUNTY": "string",
            "USPS_ZIP_PREF_CITY": "string",
            "USPS_ZIP_PREF_STATE": "string",
            "RES_RATIO": "float64",
            "BUS_RATIO": "float64",
            "OTH_RATIO": "float64",
            "TOT_RATIO": "float64",
        },
        "keys": {
            "county_fips": "COUNTY",
        },
        "value_columns": {
            "zip_code": "ZIP",
            "state_cap": "USPS_ZIP_PREF_STATE",
            "res_ratio": "RES_RATIO",
            "business_ratio": "BUS_RATIO",
            "other_ratio": "OTH_RATIO",
            "total_ratio": "TOT_RATIO",
        },
        # Final coercion for canonical columns (only needed for constructed columns; read cols already typed)
        "dtypes": {
            "county_fips": "string",
            "zip_code": "string",
            "state_cap": "string",
            "res_ratio": "float64",
            "business_ratio": "float64",
            "other_ratio": "float64",
            "total_ratio": "float64",
        },
    },
    "fips_to_county": {
        "path": "data/raw_data/zip_county_transformation/all-geocodes-v2024.xlsx",
        "format": "xlsx",
        "vintage": 2024,
        "sheet": "all_geocodes_v2024",
        "skiprows": 4,
        # Read as string so filter/combine don't lose leading zeros; Summary Level as string for filter match
        "read_dtypes": {
            "Summary Level": "string",
            "State FIPS Code": "string",
            "County FIPS Code": "string",
            "County Subdivision FIPS Code": "string",
            "Place FIPS Code": "string",
            "Consolidated City FIPS Code": "string",
            "Area Name": "string",
        },
        "filters": {
            "Summary Level": "050"
        },
        "keys": {
            "county_fips": "County FIPS Code"
        },
        "combine_columns": {
            "county_fips": {
                "from": ["State FIPS Code", "County FIPS Code"],
                "method": "concat_zfill",
                "zfill": [2, 3],
                "dtype": "string"
            }
        },
        "post_filters": {
            "county_fips_not_ending_with": "000"
        },
        "value_columns": {
            "county_name": "Area Name",
        },
        "dtypes": {
            "county_fips": "string",
            "county_name": "string",
        },
    }
}