-- =======================================================================
-- SQL Logic for Processing SKU Changes (Log and Update Master)
-- This logic assumes we process ALL records from sku_actual_changes in one batch.
-- NOTE: In a transaction system (like PostgreSQL/SQL Server), use a PROCEDURE
-- or a single MERGE statement for atomic execution.
-- =======================================================================

-- STEP 1: Insert New Records into sku_log
-- We join the ACTUAL changes (T1) with the current MASTER state (T2) to get
-- the correct 'old price' and determine the 'new_sku_code' for logging.


CREATE OR REPLACE PROCEDURE process_sku_weekly_changes(
	p_valid_from date,
	p_valid_to date
)
LANGUAGE plpgsql
AS $$
BEGIN

    -- STEP 1: Insert New Records into sku_log
    -- Joins the MASTER (source of 'old' state) with ACTUAL (source of new state)
    INSERT INTO sku_log (
		cat_1_name,
        store,
		old_product_code,
        old_sku_code,
		old_unit,
		old_price,
		product_name,
		new_product_code,
        new_sku_code,
		new_unit,
        new_price,
        price_difference,
        valid_from,
        valid_to
    )
    SELECT
		T1.cat_1_name,
        T1.store_id,
		T2.product_code,
        T2.barcode AS old_sku_code,
		T1.old_unit,
		T2.selling_price,
		T2.product_name,
        -- Determine the NEW_SKU_CODE for the log based on user requirement:
        COALESCE(T1.new_product_code, T2.product_code) AS new_product_code,
		COALESCE(T1.new_barcode, T2.barcode) AS new_sku_code,
		COALESCE(T1.new_unit, T1.old_unit) AS new_unit,
        T1.new_price AS new_price,
        T1.new_price - T2.selling_price AS price_difference,
        COALESCE(T1.valid_from, p_valid_from) as valid_from,
        COALESCE(T1.valid_to, p_valid_to) as valid_to
	FROM
    	sku_master T2 -- MASTER ở bên trái để giữ 1100 hàng
	LEFT JOIN
    	sku_actual_changes T1
	ON
    	T1.store_id = T2.store AND T1.old_sku_code = T2.barcode;


    -- STEP 2: Update sku_master with the New SKU Code and New Price
    -- This step ensures the MASTER reflects the latest state for the next run.
    UPDATE sku_master AS sm
    SET
		product_code = T1.new_product_code_final,
        barcode = T1.new_sku_code_final,
        selling_price = T1.new_price,
        valid_from = T1.valid_from
    FROM
        (
            SELECT
                sac.store_id,
                sac.old_sku_code,
                sac.new_price,
                sac.valid_from,
                -- Determine the final SKU code that should be written back to MASTER
                COALESCE(sac.new_barcode, sac.old_sku_code) AS new_sku_code_final,
				COALESCE(sac.new_product_code, sac.old_product_code) AS new_product_code_final
            FROM
                sku_actual_changes sac
        ) AS T1
    WHERE
        sm.store = T1.store_id
        -- Match the Master record using the OLD SKU code currently stored there
        AND sm.barcode = T1.old_sku_code;

    -- STEP 3: Cleanup
    -- Delete the processed records from the staging table.
    DELETE FROM sku_actual_changes;

    -- Transaction control is automatic within the procedure block.

END;
$$;