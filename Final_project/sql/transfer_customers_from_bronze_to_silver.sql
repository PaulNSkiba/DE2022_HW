DELETE FROM `{{ params.project_id }}.2_silver.customers`
WHERE DATE(_logical_dt) = "{{ ds }}";

INSERT   `{{ params.project_id}}.2_silver.customers` (
        client_id,
        first_name,
        last_name,
        email,
        registration_date,
        state,
        _id,
        _logical_dt
        )
SELECT
        CAST(Id AS INTEGER),
        FirstName,
        LastName,
        Email,
        CAST(REPLACE(RegistrationDate, '/', '-') AS DATE),
        State,
        _id,
        _logical_dt
FROM    `{{ params.project_id}}.1_bronze.customers`
WHERE   DATE(_logical_dt) = "{{ ds }}";

