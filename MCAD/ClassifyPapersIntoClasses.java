package mcad;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClassifyPapersIntoClasses {

	public static class InputMapper extends Mapper<Object, Text, Text, Text> {


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String paperID = value.toString().split("\t")[0];
			String paperFieldCode = value.toString().split("\t")[1];
			
			context.write(new Text(paperID), new Text(paperFieldCode));
		}
	}

	public static class ClassifierReducer extends Reducer<Text, Text, Text, Text> {
		// private IntWritable result = new IntWritable();
		public void reduce(Text paperID, Iterable<Text> allFields, Context context) throws IOException, InterruptedException {
			
			HashMap<Text, Integer> fieldCount = new HashMap<>();
			
			for (Text field: allFields) {
				if (fieldCount.get(field) == null) {
					fieldCount.put(field, 1);
				} else {
					fieldCount.put(field, fieldCount.get(field) + 1);
				}
			}
			
			int maxValueInMap = (Collections.max(fieldCount.values()));  // This will return max value in the Hashmap
	        for (Entry<Text, Integer> entry: fieldCount.entrySet()) {  // Iterate through hashmap
	            if (entry.getValue() == maxValueInMap) {
	            	context.write(paperID, entry.getKey());
	                //System.out.println(entry.getKey());     // Print the key with max value
	                break;
	            }
	        }
			
//			context.write(paperID, 1);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(wordCount.class);
		job.setMapperClass(InputMapper.class);
		job.setCombinerClass(ClassifierReducer.class);
		job.setReducerClass(ClassifierReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

/*

INPUT

0000004B	SE
0000004B	SE
0000004B	SE
0000006A	PL
00000118	ML
00000118	ALGO
00000148	ALGO
00000166	SC
0000016F	PL
00000199	ML
0000019E	NW
0000019E	AI
0000019E	SIM
000001A1	SE
000001A8	ARC
000001E6	DB
000001E6	SIM
000001FF	BIO
000001FF	BIO
00000230	SC

OUTPUT

0000004B	SE
0000006A	PL
00000118	ALGO
00000148	ALGO
00000166	SC
0000016F	PL
00000199	ML
0000019E	SIM
000001A1	SE
000001A8	ARC
000001E6	SIM
000001FF	BIO
00000230	SC

*/