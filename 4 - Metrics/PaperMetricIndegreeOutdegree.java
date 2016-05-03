package hbaseentry;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PaperMetricIndegreeOutdegree {

	public static class InputMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// process data for the row from the Result instance.
			String fromPaper = new String(value.getValue(Bytes.toBytes("from"), Bytes.toBytes("fromId")));
			String toPaper = new String(value.getValue(Bytes.toBytes("to"), Bytes.toBytes("toId")));

			context.write(new Text(toPaper), new Text("in"));
			context.write(new Text(fromPaper), new Text("out"));

		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}

		@Override
		protected void reduce(Text paper, Iterable<Text> type, Context context)
				throws IOException, InterruptedException {

			int countin=0,countout=0;
			for (Text degree : type) {
				if (degree.toString().equals("in")) countin++;
				else countout++;
			}
			
			//System.out.println(fromPaper + "\t" + count);

			Put put = new Put(Bytes.toBytes(paper.toString()));
			put.add(Bytes.toBytes("metrics"), Bytes.toBytes("indegree"), Bytes.toBytes(countin + ""));
			put.add(Bytes.toBytes("metrics"), Bytes.toBytes("outdegree"), Bytes.toBytes(countout + ""));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(paper.toString())), put);

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.20");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "CreateCommunityGraph");

		job.setJarByClass(PaperMetricIndegreeOutdegree.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();

		TableMapReduceUtil.initTableMapperJob("citationNetworkAdvNew", scan, InputMapper.class, Text.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob("PaperAbout", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}
}
