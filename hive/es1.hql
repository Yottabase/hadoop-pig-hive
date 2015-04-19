DROP table rows;

CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../data/generator/sample/esempio.txt' OVERWRITE INTO TABLE rows;


select explode(split(substr(row, LOCATE(',', row) + 1 ), ',')) as product from rows;
