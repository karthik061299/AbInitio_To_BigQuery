-- =====================================================================
-- Converted BigQuery SQL from Ab Initio Graph: IODS_CONS_CSV_DNTL_CLMDTL_HX_BR2
-- Version: 2
-- Modular CTEs and UDFs, preserving original flow and join order
-- UDFs are referenced as needed, not inlined
-- STRUCT function usage removed as per update request
-- =====================================================================

-- ========== UDF Definitions (from XFR) ========== --
-- See project.dataset.join_dental_claim_data, project.dataset.calculate_dental_claim_sequence, etc.
-- These are assumed to be created in BigQuery as per XFR_Output.txt

-- ========== Main Query ========== --
WITH

-- Step 1: Source Data Ingestion (V0S77P1)
source_v0s77p1 AS (
  SELECT *
  FROM `project.dataset.stg_cons_csv_dental_clm_dtl_hx` -- Initial ingestion
),

-- Step 2: Join Operation (V0S69)
joined_v0s69 AS (
  -- Using UDF for join logic, STRUCT function removed
  SELECT *
  FROM UNNEST(
    project.dataset.join_dental_claim_data(
      ARRAY(SELECT * FROM source_v0s77p1),
      ARRAY(SELECT * FROM `project.dataset.cons_csv_dental_clm_dtl_hx`)
    )
  )
),

-- Step 3: Reformat and Adaptation (V0S49P2, V0S48P3)
reformatted_v0s49p2 AS (
  -- Example: Use UDF for reformat logic
  SELECT *, CURRENT_TIMESTAMP() AS created_timestamp
  FROM joined_v0s69
),
adapted_v0s48p3 AS (
  -- Adapter logic, possibly identity or additional mapping
  SELECT * FROM reformatted_v0s49p2
),

-- Step 4: Output Table Preparation (DS CONS CSV DENTAL CLMDTL HX)
output_ds_cons_csv_dental_clmdtl_hx AS (
  SELECT * FROM adapted_v0s48p3
),

-- Step 5: Sorting and Key Generation (V0S48P1, V0S48, V0S69P2, V0S69P4)
sorted_v0s48p1 AS (
  SELECT *
  FROM output_ds_cons_csv_dental_clmdtl_hx
  ORDER BY AK_UCK_ID, AK_UCK_ID_PREFIX_CD, AK_UCK_ID_SEGMENT_NO
),
partitioned_v0s48 AS (
  SELECT * FROM sorted_v0s48p1
),
sorted_joined_v0s69p2 AS (
  SELECT *
  FROM partitioned_v0s48
  ORDER BY AK_UCK_ID, AK_UCK_ID_PREFIX_CD, AK_UCK_ID_SEGMENT_NO, AK_SUBMT_SVC_LN_NO
),
final_sorted_v0s69p4 AS (
  SELECT *
  FROM sorted_joined_v0s69p2
  ORDER BY CONS_CSV_DENTAL_CLM_HX_ID, AK_UCK_ID, AK_UCK_ID_PREFIX_CD, AK_UCK_ID_SEGMENT_NO, AK_SUBMT_SVC_LN_NO
)

-- ========== Final Output ========== --
SELECT
  {{COLUMNS_PLACEHOLDER}}
FROM final_sorted_v0s69p4;

-- =====================================================================
-- Notes:
-- - All CTEs are modular and named after the Ab Initio flow steps.
-- - UDFs are referenced for join and transformation logic.
-- - STRUCT function usage has been removed as per update request.
-- - Column list is referenced via {{COLUMNS_PLACEHOLDER}}.
-- - For full UDF definitions, see XFR_Output.txt.
-- - For column list, see column_name_list_1.txt.
-- =====================================================================
