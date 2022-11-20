DELETE FROM `{{ params.project_id }}.1_bronze.customers`
WHERE DATE(_logical_dt) = "{{ ds }}";

MERGE   `{{ params.project_id}}.1_bronze.customers` T
USING   (SELECT DISTINCT Id, FirstName, LastName, Email, RegistrationDate, State FROM customers_csv) S
ON  (
        T.Id = S.Id
     )
WHEN matched THEN
UPDATE
    SET
    T.FirstName = IFNULL(T.FirstName, S.FirstName),
    T.LastName = IFNULL(T.LastName, S.LastName),
    T.State = IFNULL(T.State, S.State),
    T.Email = IFNULL(T.Email, S.Email),
    T.RegistrationDate = IFNULL(T.RegistrationDate, S.RegistrationDate)

WHEN NOT matched THEN
    INSERT  (Id, FirstName, LastName, Email, RegistrationDate, State, _id, _logical_dt)
    VALUES  (Id, FirstName, LastName, Email, RegistrationDate, State, GENERATE_UUID(), CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) )
;
