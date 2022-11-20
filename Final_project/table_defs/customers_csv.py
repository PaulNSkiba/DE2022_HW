customers_csv = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "Id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "FirstName",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "LastName",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "Email",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "RegistrationDate",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "State",
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
            "/raw/customers"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d')}}"
            "/*__customers.csv"
        )
    ]
}

