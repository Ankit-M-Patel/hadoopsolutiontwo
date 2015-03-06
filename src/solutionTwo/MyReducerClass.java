package solutionTwo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException{
	
		
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}
		try {
			context.write(key, new IntWritable(sum));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}	
}
