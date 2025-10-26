CREATE OR REPLACE PROCEDURE load_sku_changes_from_file(
    p_file_path TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- STEP 1: Truncate the staging table to ensure idempotency (no double-processing)
    TRUNCATE TABLE sku_actual_changes;

    -- STEP 2: Load data from the specified server file path into the staging table.
    -- NOTE: The file must be accessible on the PostgreSQL server's filesystem,
    -- and the user executing this procedure must have appropriate permissions (e.g., pg_read_server_files).
    EXECUTE format(
        'COPY sku_actual_changes (store_id, old_sku_code, new_sku_code, new_price, valid_from, valid_to) 
         FROM %L DELIMITER '','' CSV HEADER;',
        p_file_path
    );
    
    -- Optional: Log the number of rows inserted
    RAISE NOTICE 'Successfully loaded % rows into sku_actual_changes from file: %', 
        (SELECT COUNT(*) FROM sku_actual_changes), p_file_path;

    COMMIT; -- Commit the transaction if necessary (though usually handled by external environment)
END;
$$;