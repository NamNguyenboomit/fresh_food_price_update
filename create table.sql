-- Master Table: Stores the CURRENTLY accepted SKU and Price
-- This table is the source of truth for the 'old' values during processing.
-- master table for storing data B2B sent to MD for confirmation
CREATE TABLE sku_master (
	cat_1_name VARCHAR(20) NULL,
    product_code VARCHAR(50), -- Logical ID linking all versions
    barcode VARCHAR(50) NOT NULL,
    product_name VARCHAR(300) NULL,
    selling_price DECIMAL(10, 2) NULL,
    store VARCHAR(20) NOT NULL, --note store is HNC, don't use BDH
    -- Date when the current state became effective in the master table
    valid_from DATE NOT NULL,
    PRIMARY KEY (store, barcode)
);



-- Actual Table: Input of changes reported for a period
-- this is the table MD confirm for HO using in 1 week
-- This table drives the logging and updates for the given period.
CREATE TABLE sku_actual_changes (
    actual_id SERIAL PRIMARY KEY,
    cat_1_name VARCHAR(20) NULL,
    -- The SKU code expected to be currently active in MASTER
    old_product_code VARCHAR(50) null,
    old_sku_code VARCHAR(50) NOT NULL,
    old_unit VARCHAR(10) NULL,
    store_id VARCHAR(20) NULL,
    -- The reported old price (often redundant if MASTER is trusted, but included for completeness)
    old_price DECIMAL(10, 2) NULL,
    -- The new SKU code. NULL means the SKU code did not change.
    new_product_code VARCHAR(50) null,
    new_barcode VARCHAR(50) NULL,
    new_unit VARCHAR(10) NULL,
    new_price DECIMAL(10, 2) NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NULL -- If NULL, the change is indefinite
);


-- Log Table: Records every transaction and change, tracking the old and new state
-- This is your historical log table.
CREATE TABLE sku_log (
    log_id SERIAL PRIMARY KEY,
    log_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    cat_1_name VARCHAR(20) null,
    store VARCHAR(20) NULL,
    -- The SKU code *before* the transaction (from SKU_MASTER)
    old_product_code VARCHAR(50) null,
    old_sku_code VARCHAR(50) NOT NULL,
    old_unit VARCHAR(10) null,
    old_price DECIMAL(10, 2) NULL,
    product_name VARCHAR(300) NULL,
    -- The SKU code *after* the transaction (from SKU_ACTUAL_CHANGES or OLD_SKU_CODE)
    new_product_code VARCHAR(50) null,
    new_sku_code VARCHAR(50) NOT NULL,
    new_unit VARCHAR(10) null,
    new_price DECIMAL(10, 2) NULL,
    -- Calculated difference: new_price - old_price
    price_difference DECIMAL(10, 2) NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NULL
);

drop table sku_log 

