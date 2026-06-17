SOURCES_ZIP = {
    "electricity_price": {
        "vintage": 2023,
        "sources": [
            {
                "path": "data/raw_data/electricity_price_2023/iou_zipcodes_2023.csv",
                "format": "csv",
            },
            {
                "path": "data/raw_data/electricity_price_2023/non_iou_zipcodes_2023.csv",
                "format": "csv",
            },
        ],
        "concat": True,  # concatenate the two sources
        # Apply at read time so no later step alters values (e.g. leading zeros in zip)
        "read_dtypes": {
            "zip": "string",
            "ownership": "string",
            "comm_rate": "float64",
            "ind_rate": "float64",
            "res_rate": "float64",
        },
        "keys": {
            "zip_code": "zip",
        },
        "value_columns": {
            "ownership": "ownership",
            "commercial_price": "comm_rate",
            "industrial_price": "ind_rate",
            "residential_price": "res_rate",
        },
        # Final coercion for canonical columns (string for joins, float for rates)
        "dtypes": {
            "zip_code": "string",
            "ownership": "string",
            "commercial_price": "float64",
            "industrial_price": "float64",
            "residential_price": "float64",
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