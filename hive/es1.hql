DROP TABLE rows;

CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';


LOAD DATA LOCAL INPATH '../data/generator/sample/esempio.txt' OVERWRITE INTO TABLE rows;

INSERT OVERWRITE LOCAL DIRECTORY '../data/output/hive/es1_SimpleBilling'
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
SELECT p.product, count(*) as count
FROM 
	(SELECT explode(split(substr(row, LOCATE(',', row) + 1 ), ',')) AS product FROM rows) p
GROUP BY p.product
ORDER BY count DESC;
