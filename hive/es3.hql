DROP table rows;

CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../data/generator/sample/esempio.txt' OVERWRITE INTO TABLE rows;

SELECT p1.product, p2.product, count(*) as count
FROM (select explode(split(substr(row, LOCATE(',', row) + 1 ), ',')) as product from rows) p1
JOIN (select explode(split(substr(row, LOCATE(',', row) + 1 ), ',')) as product from rows) p2
WHERE p1.product < p2.product
GROUP BY p1.product, p2.product
ORDER BY count DESC

