DROP table products;

CREATE TABLE products (data STRING, item ARRAY<STRING>)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/generator/sample/esempio.txt' OVERWRITE INTO TABLE products;

select * from products;
