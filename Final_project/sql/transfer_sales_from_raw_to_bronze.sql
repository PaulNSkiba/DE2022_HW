DELETE FROM `{{ params.project_id }}.1_bronze.sales`
WHERE DATE(_logical_dt) = "{{ ds }}";

INSERT  `{{ params.project_id}}.1_bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    _id,
    _logical_dt
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    GENERATE_UUID() as _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) _logical_dt
FROM sales_csv;
