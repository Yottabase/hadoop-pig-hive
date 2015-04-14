

myinput = LOAD '../data/generator/sample/esempio.txt' USING PigStorage(',') ;

products = FOREACH myinput GENERATE FLATTEN(TOBAG($1..));

grouped = GROUP products BY $0;

counts = FOREACH grouped GENERATE group, COUNT(products);

sorted = ORDER counts BY $1 DESC; 

STORE sorted INTO '../data/output/pig/es1_SimpleBilling' USING PigStorage();
