

myinput = LOAD '../data/generator/sample/esempio.txt' USING PigStorage(',') ;

filtered = FILTER myinput BY $0 matches '2015-[012]-.*';

bydate = FOREACH filtered GENERATE STRSPLIT($0, '-').$1, FLATTEN(TOBAG($1..));

grouped = GROUP bydate BY ($1, $0);

counted = FOREACH grouped GENERATE group.$0, (group.$1+1, COUNT(bydate));

groupedByMonth =  GROUP counted BY $0;

STORE groupedByMonth INTO '../data/output/pig/es2_QuarterAggregation' USING PigStorage();
