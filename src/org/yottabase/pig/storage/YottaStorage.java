package org.yottabase.pig.storage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.mapreduce.RecordWriter;

public class YottaStorage extends PigStorage {
	
	protected RecordWriter writer = null;
	
	/*
	
	@Override
	public void putNext(Tuple f) throws IOException {
		
		
		
		// TODO Auto-generated method stub
		
		
		
		
		
		Text text = new Text(mOut.toByteArray());
		
		
		super.writer.write(null, text);
		
		
	}
	*/
	
	
	@Override
    public void putNext(Tuple f) throws IOException {
		f.set(0, new Text("ciao"));
		super.putNext(f);
        /*try {
            this.writer.write(null, new Tuple new Text("ciao"));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }*/
    }
	
	@Override
    public void prepareToWrite(RecordWriter writer) {
        this.writer = writer;
        super.prepareToWrite(writer);
    }
	
    
}