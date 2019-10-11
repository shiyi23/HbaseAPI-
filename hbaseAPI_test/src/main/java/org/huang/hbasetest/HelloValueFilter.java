package org.huang.hbasetest;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


public class HelloValueFilter {
    public static void main(String[] args) throws URISyntaxException,IOException,InterruptedException {

        Configuration config = HBaseConfiguration.create();
        //添加必要的配置文件: hbase-site.xml、core-site.xml、hdfs-site.xml
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));

        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));

        try(Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("hsy_table"));
            Scan scan = new Scan();

            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("mycf"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("Wang"));
            scan.setFilter(filter);

            ResultScanner rs = table.getScanner(scan);
            for (Result r: rs) {
                String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
                System.out.println(name);
            }
            rs.close();

        }


    }

}
