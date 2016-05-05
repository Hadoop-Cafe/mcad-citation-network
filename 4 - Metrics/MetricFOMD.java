package hbaseentry;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MetricFOMD {

	public static class InputMapper extends TableMapper<Text, Text> {

	//	HTable htablePaperAbout;

		//@Override
//		protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			// super.setup(context);
//			Configuration config = HBaseConfiguration.create();
//			config.set("hbase.zookeeper.quorum", "172.17.25.20");
//			config.set("hbase.zookeeper.property.clientPort", "2183");
//
//			htablePaperAbout = new HTable(config, "PaperAbout");
//		}

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// process data for the row from the Result instance.
			// String fromPaper = new
			// String(value.getValue(Bytes.toBytes("from"),
			// Bytes.toBytes("fromId")));
			String classes = new String(value.getValue(Bytes.toBytes("classes"), Bytes.toBytes("classes")));
			int indegree = Integer
					.parseInt(new String(value.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("indegree"))));

			int MEDIAN_INDEGREE = 0;
			Text TEXT_ONE = new Text("1");

			if (indegree > MEDIAN_INDEGREE) {
				for (String someclass : classes.substring(1).split(",")) {
					context.write(new Text(someclass), TEXT_ONE);
				}
			}

		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		Map<String, Integer> totalPaperCount = new HashMap<>();
		int totalPapers = 0;

		@Override
		protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.20");
			config.set("hbase.zookeeper.property.clientPort", "2183");

			HTable htableCommunityAbout = new HTable(config, "CommunityAbout");
			
			Scan scan = new Scan();
			ResultScanner scanner = htableCommunityAbout.getScanner(scan);
			
			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				String className = Bytes.toString(result.getRow());
				String classCount = Bytes.toString(result.getValue(Bytes.toBytes("count"), Bytes.toBytes("count")));
			    totalPaperCount.put(className, Integer.parseInt(classCount));
			    totalPapers += Integer.parseInt(classCount);
			}
			
			
		}

		@Override
		protected void reduce(Text someClass, Iterable<Text> ValueOne, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (Text One : ValueOne) {
				++count;
			}
			double fomd = (double)count / totalPaperCount.get(someClass.toString());
			
			Put put = new Put(Bytes.toBytes(someClass.toString()));
			put.add(Bytes.toBytes("metrics"), Bytes.toBytes("FOMD"), Bytes.toBytes(fomd + ""));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(someClass.toString())), put);

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.20");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "CreateCommunityGraph");

		job.setJarByClass(MetricFOMD.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();

		TableMapReduceUtil.initTableMapperJob("PaperAbout", scan, InputMapper.class, Text.class, Text.class, job);

		TableMapReduceUtil.initTableReducerJob(".", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}
}
