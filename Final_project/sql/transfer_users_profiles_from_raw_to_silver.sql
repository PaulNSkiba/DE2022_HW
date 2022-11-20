DELETE FROM `{{ params.project_id }}.2_silver.user_profiles`
WHERE DATE(_logical_dt) = "{{ ds }}";

INSERT  `{{ params.project_id}}.2_user_profiles` (
    email,
    full_name,
    state,
    birth_date,
    phone_number,
    _id,
    _logical_dt
)
SELECT
    email,
    full_name,
    state,
    birth_date,
    phone_number,
    GENERATE_UUID() as _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) _logical_dt
FROM user_profiles;
