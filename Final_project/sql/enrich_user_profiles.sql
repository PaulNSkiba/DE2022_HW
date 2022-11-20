DELETE FROM `{{ params.project_id }}.3_gold.user_profiles_enriched`
WHERE DATE(_logical_dt) = "{{ ds }}";

MERGE   `{{ params.project_id}}.3_gold.user_profiles_enriched` T
USING   (   SELECT DISTINCT  client_id,
                             C.email,
                             registration_date,
                             U.state,
                             LEFT(full_name, INSTR(full_name, ' ')) first_name,
                             RIGHT(full_name, LENGTH(full_name) - INSTR(full_name, ' ')) last_name,
                             birth_date,
                             phone_number,
                             C._id,
                             C._logical_dt
            FROM `{{ params.project_id}}.2_silver.customers` C
                 JOIN `{{ params.project_id}}.2_silver.user_profiles` U ON C.email = U.email ) S
ON  (
        T.email = S.email
     )
WHEN matched THEN
UPDATE
    SET
    T.first_name = S.first_name,
    T.last_name = S.last_name,
    T.state = S.state,
    T.birth_date = s.birth_date,
    T.phone_number = S.phone_number
WHEN NOT matched THEN
    INSERT  (client_id, first_name, last_name, email, registration_date, state, birth_date, phone_number, _id, _logical_dt)
    VALUES  (client_id, first_name, last_name, email, registration_date, state, birth_date, phone_number, _id, _logical_dt)
;
