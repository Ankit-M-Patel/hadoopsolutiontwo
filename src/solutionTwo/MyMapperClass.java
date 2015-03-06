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

		String refinedString = refineValue(value,"sherlock","holmes");		
		Pattern sherlockHolmes = Pattern.compile("sherlockholmes");        //Write both word as single which you want to find
		Matcher sherlockHomesMatcher = sherlockHolmes.matcher(refinedString);

		String refinedMulliganStr = refineValue(value,"buck","mulligan");		
		Pattern buckMulligan = Pattern.compile("buckmulligan");        //Write both word as single which you want to find
		Matcher buckMulliganMatcher = buckMulligan.matcher(refinedMulliganStr);

		
		try {

			while (sherlockHomesMatcher.find()) {

				System.out.println("*********Word Matched*********");
				System.out.println("*************" + refinedString);
				String matchedKey = sherlockHomesMatcher.group().toLowerCase();
				// remove special characters and digits
				if (!Character.isLetter(matchedKey.charAt(0))
						|| Character.isDigit(matchedKey.charAt(0))) {
					continue;
				}

				context.write(new Text(matchedKey), one);
			}



			while (buckMulliganMatcher.find()) {

				System.out.println("*********Word Matched*********");
				System.out.println("*************" + refinedMulliganStr);
				String matchedKey = buckMulliganMatcher.group().toLowerCase();
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

	private String refineValue(Text value, String firstWord , String secondWord) {
		String line = value.toString().toLowerCase();
		line  = line.replaceAll("[^a-zA-Z]", " ");

		Pattern pattern = Pattern.compile("("+firstWord+"|"+secondWord+")");
		Matcher matcher = pattern.matcher(line);

		String refinedString = "";

		while(matcher.find())
		{
			refinedString += matcher.group(1);
		}
		return refinedString;
	}	
}

