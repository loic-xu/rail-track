{{ config(materialized='table') }}

WITH distinct_dates AS (
    SELECT DISTINCT
        date_key,
        CAST(strptime(CAST(date_key AS VARCHAR), '%Y%m%d') AS DATE) AS full_date
    FROM {{ ref('stg_train_stops') }}
    WHERE date_key IS NOT NULL
)

SELECT
    date_key,
    full_date,
    CAST(EXTRACT(year FROM full_date) AS INTEGER) AS year,
    CAST(EXTRACT(month FROM full_date) AS INTEGER) AS month,
    CAST(EXTRACT(day FROM full_date) AS INTEGER) AS day,
    CAST(EXTRACT(quarter FROM full_date) AS INTEGER) AS quarter,
    dayname(full_date) AS day_name,
    monthname(full_date) AS month_name,
    CAST(EXTRACT(isodow FROM full_date) AS INTEGER) AS day_of_week,
    CASE
        WHEN EXTRACT(isodow FROM full_date) IN (6, 7) THEN true
        ELSE false
    END AS is_weekend
FROM distinct_dates
