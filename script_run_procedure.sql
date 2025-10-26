call load_sku_master_from_file('D:/LotteMart/Fresh Food Price/Template/master_20250927.csv');


call load_sku_changes_from_file('D:/LotteMart/Fresh Food Price/Template/md_file_20250927.csv');


call process_sku_weekly_changes();

delete
from sku_actual_changes 


select *
from sku_log 
where
	valid_from = '2025-09-27' and
	valid_to = '2025-10-03' and
	new_price <> 0
	

	
	