
records = LOAD '../data/generator/sample/esempio.txt' USING PigStorage(',') ;

products = FOREACH records GENERATE TOBAG($1..) AS recordProds;

pairs = FOREACH products {
	recordPairs = CROSS recordProds, recordProds;
	GENERATE FLATTEN(recordPairs) as (p1,p2);
}

-- 
assocs = FILTER pairs BY p1 < p2; 
assocsGrouped = GROUP assocs BY (p1, p2);
assocsCount = FOREACH assocsGrouped GENERATE group, (float) COUNT(assocs) as count1;

-- conta numero di transazioni in cui compare ogni prodotto
prods = FILTER pairs BY p1 == p2; 
prodsGrouped = GROUP prods BY (p1, p2);
prodsCount = FOREACH prodsGrouped GENERATE group, (float) COUNT(prods) as count2;

--
joined = JOIN assocsCount BY group.$0, prodsCount BY group.$0;

--
prodPercents = FOREACH joined GENERATE $0, $1/$3 AS percent;

-- 
sorted = ORDER prodPercents BY percent DESC; 

--
topK = LIMIT sorted 10;

dump topK;

