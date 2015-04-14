

myinput = LOAD '../data/generator/sample/esempio.txt' USING PigStorage(',') ;

filtered = FILTER myinput BY $0 matches '.*-[012]-.*';

bydate = FOREACH filtered GENERATE STRSPLIT($0, '-').$1, FLATTEN(TOBAG($1..));

grouped = GROUP bydate BY ($1, $0);

counted = FOREACH grouped GENERATE group.$0, (group.$1+1, COUNT(bydate));

groupedByMonth =  GROUP counted BY $0;

--print = FOREACH counted GENERATE $0, TOMAP($1, $2);

DUMP groupedByMonth;
