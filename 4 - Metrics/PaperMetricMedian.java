package hbaseentry;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hbaseentry.DumpFilteredCitationsToFile.InputMapper;
import hbaseentry.DumpFilteredCitationsToFile.PaperDumpReducer;

public class PaperMetricMedian {

	public static class InputMapper extends TableMapper<IntWritable, Text> {

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// process data for the row from the Result instance.
			//String indegree = Bytes.toString(value.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("indegree")));
			String indegree = new String(value.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("indegree")));
			context.write(new IntWritable(Integer.parseInt(indegree)), new Text("1"));	// send indegree as key since keys are always sorted @ end

		}
	}

	public static class PaperDumpReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for (Text field : value) {
				context.write(key, field);
			}
		}
	}

	/*
	SAMPLE OUTPUT
	1
	1
	1
	2
	2 <- MEDIAN
	3
	3
	3
	3
	*/

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.20");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "CalcMedian");

		job.setJarByClass(PaperMetricMedian.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();

		TableMapReduceUtil.initTableMapperJob("PaperAbout", scan, InputMapper.class, IntWritable.class, Text.class, job);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.setReducerClass(PaperDumpReducer.class);
		job.waitForCompletion(true);
	}
}
