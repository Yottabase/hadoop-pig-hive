package org.yottabase.billing;

import java.io.File;
import org.yottabase.utils.FileSystem;

public class GlobalMain {

	public static void main(String[] args) throws Exception {
		
		String inputPath, outputPath;
		
		if(args.length == 2){
			inputPath = args[0];
			outputPath = args[1];	
		}else{
			inputPath = "data/generator/sample/";
			outputPath = "data/output/";
		}
		
		FileSystem.deleteDirectory(new File(outputPath));
		
		//org.yottabase.billing.es1.onereducer.Main.runJob(inputPath, outputPath + "/es1_simple_billing_onered");
		
		//org.yottabase.billing.es2.Main.runJob(inputPath, outputPath + "/es2_quarter_aggregation");
		
		org.yottabase.billing.es3.Main.runJob(inputPath, outputPath);
	}

}
