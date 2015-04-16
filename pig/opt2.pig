
REGISTER '../target/bigdata-0.0.1-SNAPSHOT.jar'
DEFINE powerset org.yottabase.billing.optional.es3.PowersetUDF();

records = LOAD '../data/generator/sample/esempio.txt' USING PigStorage(',') ;

-- lista di scontrini senza data 
products = FOREACH records GENERATE TOBAG($1..) AS recordProds;

--
subsets = 
	FOREACH products {
		subset = powerset(recordProds);
		GENERATE FLATTEN(subset);
}

grouped = GROUP subsets BY $0..;

-- conta il numero di occorrenze per coppia
counts = FOREACH grouped GENERATE group, COUNT(subsets);

DUMP counts;
