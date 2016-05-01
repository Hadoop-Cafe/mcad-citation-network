package hbaseentry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class DataEntryComputerScienceFieldsTable {

	public static void main(String[] args) throws IOException {
		
		BufferedReader br = new BufferedReader(new FileReader("ComputerScienceFieldsOfStudy.txt"));
		/*
		 * 03ef3421	AI
		 * 0053eaaf	ALGO
		 */

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "172.17.25.20");
		config.set("hbase.zookeeper.property.clientPort", "2183");

		// Instantiating HTable class
		HTable hTableComputerScienceFieldsTable = new HTable(config, "ComputerScienceFieldsTable");
//		Table created as : create 'ComputerScienceFieldsTable', 'fieldId', 'communityCode'

		String line;
		while ((line = br.readLine()) != null) {
			Put p = new Put(Bytes.toBytes(line.split("\t")[0]));	// Instantiating Put class accepts a row name.
			p.add(Bytes.toBytes("communityCode"), Bytes.toBytes("communityCode"), Bytes.toBytes(line.split("\t")[1]));
			hTableComputerScienceFieldsTable.put(p);
		}
		
	}
}