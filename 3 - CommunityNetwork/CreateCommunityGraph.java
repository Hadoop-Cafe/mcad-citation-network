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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.org.apache.commons.logging.LogFactory;

import hbaseentry.ccg.InputMapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class CreateCommunityGraph {

	public static class InputMapper extends TableMapper<Text, Text> {

		private HTable htablePaperAbout;
		private Configuration config;

		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.20");
			config.set("hbase.zookeeper.property.clientPort", "2183");
			try {
				htablePaperAbout = new HTable(config, "PaperAbout");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String fromPaper = Bytes.toString(value.getValue(Bytes.toBytes("from"), Bytes.toBytes("fromId")));
			String toPaper = new String(value.getValue(Bytes.toBytes("to"), Bytes.toBytes("toId")));

			Get fromPaperRow = new Get(Bytes.toBytes(fromPaper));
			Result fromPaperRowResult = htablePaperAbout.get(fromPaperRow);

			Get toPaperRow = new Get(Bytes.toBytes(toPaper));
			Result toPaperRowResult = htablePaperAbout.get(toPaperRow);
			// byte [] value =
			// result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));
			if (!fromPaperRowResult.isEmpty() && !toPaperRowResult.isEmpty()) {	// if both the papers belong to computer science communities
				Text one = new Text("1");
				// Get communities for both the papers
				String fromClass[] = Bytes
						.toString(fromPaperRowResult.getValue(Bytes.toBytes("classes"), Bytes.toBytes("classes")))
						.substring(1).split(",");
				String toClass[] = Bytes
						.toString(toPaperRowResult.getValue(Bytes.toBytes("classes"), Bytes.toBytes("classes")))
						.substring(1).split(",");

				for (int i = 0; i < fromClass.length; i++) {
					for (int j = 0; j < toClass.length; j++) {
						context.write(new Text(fromClass[i] + "," + toClass[j]), one);	// concat with comma and send to reducer
					}
				}
			}
		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

		}

		@Override
		protected void reduce(Text classPair, Iterable<Text> ValueOne, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Text One : ValueOne) {	// each pair of form `C1,C2` counts as an edge from C1 to C2
				++count;
			}

			String fromcomm = classPair.toString().split(",")[0];	// get from community
			String tocomm = classPair.toString().split(",")[1];		// get to community

			Put put = new Put(Bytes.toBytes(classPair.toString()));
			put.add(Bytes.toBytes("from"), Bytes.toBytes("fromClass"), Bytes.toBytes(fromcomm));
			put.add(Bytes.toBytes("to"), Bytes.toBytes("toClass"), Bytes.toBytes(tocomm));
			put.add(Bytes.toBytes("edgeCount"), Bytes.toBytes("count"), Bytes.toBytes(count + ""));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(classPair.toString())), put);	// write to table

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.20");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "CreateCommunityGraphCSV");
		job.setJarByClass(CreateCommunityGraph.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();
//		 scan.setStartRow(Bytes.toBytes("0000004B0045E6AD"));
//		 scan.setStopRow(Bytes.toBytes("00000486091A501E"));
		TableMapReduceUtil.initTableMapperJob("CitationNetwork", scan, InputMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("CommunityNetwork", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}
}
