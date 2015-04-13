

myinput = LOAD '../data/generator/sample/esempio.txt' USING TextLoader() as (myword:chararray);


products = FOREACH myinput GENERATE TOKENIZE(*);

products2 = FOREACH products {
	c = foreach $0 GENERATE $0;
}

DUMP products2;


-- a = load '1.txt' as (a0, a1:chararray, a2:chararray); 
-- b = group a by a0; 
-- c = foreach b { 
    -- c0 = foreach a generate TOMAP(a1,a2); 
    -- generate c0; 
-- } 
-- dump c; 