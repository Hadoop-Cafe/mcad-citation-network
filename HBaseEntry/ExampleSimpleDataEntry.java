package hbaseentry;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleSimpleDataEntry {

	public static void main(String[] args) throws IOException {

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "172.17.25.20");
		config.set("hbase.zookeeper.property.clientPort", "2183");

		// Instantiating HTable class
		HTable hTable = new HTable(config, "emp");

		// Instantiating Put class
		// accepts a row name.
		Put p = new Put(Bytes.toBytes("row1"));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.add(Bytes.toBytes("personal data"), Bytes.toBytes("name"), Bytes.toBytes("Kaju"));

		p.add(Bytes.toBytes("personal data"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad"));

		p.add(Bytes.toBytes("professional data"), Bytes.toBytes("designation"), Bytes.toBytes("manager"));

		p.add(Bytes.toBytes("professional data"), Bytes.toBytes("salary"), Bytes.toBytes("50000"));

		// Saving the put Instance to the HTable.
		hTable.put(p);
		System.out.println("data inserted");

		// closing HTable
		hTable.close();
	}
}