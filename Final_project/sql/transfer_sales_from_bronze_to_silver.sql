DELETE FROM `{{ params.project_id }}.2_silver.sales`
WHERE DATE(purchase_date) = "{{ ds }}";

INSERT  `{{ params.project_id}}.2_silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price,
    _id,
    _logical_dt
)
SELECT
    CAST(CustomerId AS INTEGER),
    CAST(REPLACE(PurchaseDate, '/', '-') AS DATE),
    Product,
    CAST(REPLACE(REPLACE(Price, 'USD', ''), '$', '') AS INTEGER),
    _id,
    _logical_dt
FROM `{{ params.project_id}}.1_bronze.sales`
WHERE CAST(REPLACE(PurchaseDate, '/', '-') AS DATE) = "{{ ds }}";
