
records = LOAD '../data/generator/sample/esempio.txt' USING PigStorage(',') ;

products = FOREACH records GENERATE TOBAG($1..) AS recordProds;

pairs = FOREACH products {
	recordPairs = CROSS recordProds, recordProds;
	GENERATE FLATTEN(recordPairs) as (p1,p2);
} 

filtered = FILTER pairs BY p1 < p2; 

grouped = GROUP filtered BY (p1, p2);

counts = FOREACH grouped GENERATE group, COUNT(filtered) as count;

sorted = ORDER counts BY count DESC;  

topK = LIMIT sorted 10;

dump topK;

