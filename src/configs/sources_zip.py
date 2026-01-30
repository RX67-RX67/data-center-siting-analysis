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
        "keys": {
            "zip_code": "zip",
        },
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