package solutionTwo;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {


	private final static IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

		// First Convert to lower or upper for compare 
		// Remove all the extra characters
		// Count patterns 

		String line = value.toString().toLowerCase();
		line  = line.replaceAll("[^a-zA-Z]", " ");

		Pattern pattern = Pattern.compile("(see|deduce)");
		Matcher matcher = pattern.matcher(line);

		String refinedString = "";

		while(matcher.find())
		{
			refinedString += matcher.group(1);
		}

		int sum = 0;

		Pattern p = Pattern.compile("seededuce");        //Write your word together that you want to find

		Matcher m = p.matcher(refinedString);

		try {

			while (m.find()) {
				String matchedKey = m.group().toLowerCase();
				// remove special characters and digits
				if (!Character.isLetter(matchedKey.charAt(0))
						|| Character.isDigit(matchedKey.charAt(0))) {
					continue;
				}

			context.write(new Text(matchedKey), one);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}

