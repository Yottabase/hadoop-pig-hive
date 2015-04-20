add jar hive-contrib-0.14.0.jar; 
drop temporary function row_sequence; 
create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';




-- crea tabella dove carica le righe dell'input (record scontrino)
DROP TABLE rows; 
CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';

-- crea tabella con id univoco scontrino ed array prodotti
DROP TABLE receiptRows;        
CREATE TABLE receiptRows (id INT, products ARRAY<STRING>)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';

-- carica le righe
LOAD DATA LOCAL INPATH '../data/generator/sample/esempio.txt' OVERWRITE INTO TABLE rows;

-- 
INSERT INTO TABLE receiptRows 
SELECT row_sequence(), SPLIT(SUBSTR(row, LOCATE(',', row) + 1 ), ',') FROM rows;

SELECT * FROM receiptRows;