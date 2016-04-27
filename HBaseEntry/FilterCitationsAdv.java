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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
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
import org.apache.hadoop.hbase.client.Result;

public class FilterCitationsAdvance {

    public static class InputMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HTable htable;
        private Configuration config;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // configuring hbase
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "172.17.25.20");
            config.set("hbase.zookeeper.property.clientPort", "2183");
            try {
                htable = new HTable(config, "UniquePaperTable");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String fromPaper = value.toString().split("\t")[0];
            String toPaper = value.toString().split("\t")[1];

            // check if both papers exist in table citationNetwork
            // Instantiating Get class
            Get g = new Get(Bytes.toBytes(fromPaper.toString()));
            // Reading the data
            Result result = htable.get(g);
            if (!result.isEmpty()) {
                // find the toPaper
                Get g1 = new Get(Bytes.toBytes(toPaper.toString()));
                Result result1 = htable.get(g1);
                if (!result1.isEmpty()) {
                    // write to Hbase table
                    String mergeKey = fromPaper.toString().trim() + toPaper.toString().trim();
                    Put put = new Put(Bytes.toBytes(mergeKey));
                    put.add(Bytes.toBytes("from"), Bytes.toBytes("fromId"), Bytes.toBytes(fromPaper.toString()));
                    put.add(Bytes.toBytes("to"), Bytes.toBytes("toId"), Bytes.toBytes(toPaper.toString()));

                    context.write(new Text(fromPaper), new Text(toPaper));
                }

            }
        }
    }

    public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text fromPaper, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text toPaper : values) {
                String mergeKey = fromPaper.toString().trim() + toPaper.toString().trim();
                Put put = new Put(Bytes.toBytes(mergeKey));
                put.add(Bytes.toBytes("from"), Bytes.toBytes("fromId"), Bytes.toBytes(fromPaper.toString()));
                put.add(Bytes.toBytes("to"), Bytes.toBytes("toId"), Bytes.toBytes(toPaper.toString()));

                context.write(new ImmutableBytesWritable(Bytes.toBytes(mergeKey)), put);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.17.25.20");
        conf.set("hbase.zookeeper.property.clientPort", "2183");
        Job job = new Job(conf, "JOB_NAME");
        job.setJarByClass(FilterCitationsAdvance.class);
        job.setMapperClass(InputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        TableMapReduceUtil.initTableReducerJob("citationNetworkAdv", PaperEntryReducer.class, job);
        job.setReducerClass(PaperEntryReducer.class);
        job.waitForCompletion(true);
    }
}
