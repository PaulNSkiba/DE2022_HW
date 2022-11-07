sales_csv = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "CustomerId",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "PurchaseDate",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "Product",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "Price",
                "type": "STRING",
                "mode": "NULLABLE"
            }

        ]
    },
    "csvOptions": {
        "allowJaggedRows": False,
        "allowQuotedNewLines": False,
        "maxBadRecords": 0,
        "encoding": "UTF-8",
        "quote": "\"",
        "fieldDelimiter": ",",
        "skipLeadingRows": 1
    },
    "sourceFormat": "CSV",
    "sourceUris": [
        (
            "gs://{{params.dl_bucket}}"
            "/raw/sales"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d')}}"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d')}}__sales.csv"
        )
    ]
}

