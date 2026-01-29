SOURCES_REFERENCE = {
    "zip_to_fips":{
        "path": "data/raw_data/zip_county_transformation/ZIP_COUNTY_092025.xlsx",
        "format": "xlsx",
        "sheet": "Export Worksheet",
        "keys": {
            "county_fips":"COUNTY",
        },
        "value_columns": {
            "zip_code":"ZIP",
            "state_cap":"USPS_ZIP_PRE_STATE",
            "res_ratio":"RES_RATIO",
            "business_ratio":"BUS_RATIO",
            "other_ratio":"OTH_RATIO",
            "total_ratio":"TOT_RATIO",
        },
    },
    "fips_to_county": {
        "path": "data/raw_data/zip_county_transformation/all-geocodes-v2024.xlsx",
        "format": "xlsx",
        "sheet": "all_geocodes_v2024",
        "skiprows": 4,  
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
        }
    }
}