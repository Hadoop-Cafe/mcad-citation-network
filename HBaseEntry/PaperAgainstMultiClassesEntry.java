package hbaseentry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PaperAgainstMultiClassEntry {

	public static class InputMapper extends Mapper<LongWritable, Text, Text,Text>  {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String paperId = value.toString().split("\t")[0];
			String paperClass = value.toString().split("\t")[1];
			
			context.write(new Text(paperId), new Text(paperClass));
		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
		
		@Override
		protected void reduce(Text paperID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text classID : values) {
				String mergeKey = paperID.toString()+ classID.toString();
				 Put put = new Put(Bytes.toBytes(mergeKey));
		         put.add(Bytes.toBytes("paperID"),Bytes.toBytes("paperID"),Bytes.toBytes(paperID.toString()));
		         put.add(Bytes.toBytes("class"),Bytes.toBytes("class"),Bytes.toBytes(classID.toString()));
		         context.write(new ImmutableBytesWritable(Bytes.toBytes(mergeKey)), put);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.20");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		Job job = new Job(conf,"JOB_NAME");
		    job.setJarByClass(PaperAgainstMultiClassEntry.class);
		    job.setMapperClass(InputMapper.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
//		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    TableMapReduceUtil.initTableReducerJob("PaperAgainstMultiClasses", PaperEntryReducer.class, job);
		    job.setReducerClass(PaperEntryReducer.class);
		            job.waitForCompletion(true);
	}
}