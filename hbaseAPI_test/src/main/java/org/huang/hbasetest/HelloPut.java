package org.huang.hbasetest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class HelloPut {
    public static void main(String[] args) throws URISyntaxException,IOException {

        Configuration config = HBaseConfiguration.create();

        //添加必要的配置文件: hbase-site.xml、core-site.xml、hdfs-site.xml
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));

        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));

//        连接hbase集群的代码发生在下面这一行的try的()里
        try(Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("hsy_table2"));

            Put put = new Put(Bytes.toBytes("row1"));

            put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), Bytes.toBytes("james"));

            table.put(put);

            /**
             * append方法: 它仅仅只做一件简单的事
             * 情， 那就是往列上的字节数组添加字节。
             */
            Append append = new Append(Bytes.toBytes("row1"));

            append.add(Bytes.toBytes("mycf"), Bytes.toBytes("name"), Bytes.toBytes("Huang"));

            table.append(append);

            /**
             * increment方法: 使得某个数据增加指定值
             * 注意：在执行increment操作之前必须要保证在Hbase中存储的数据是long格式的，而不能是字符串格式
             */

            //为row2行添加一个列mycf:age, 值是long类型的6
            Put put2 = new Put(Bytes.toBytes("row2"));

            put2.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(6L));

            table.put(put2);

            //给mycf:age加10

            Increment inc = new Increment(Bytes.toBytes("row2"));

            inc.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), 10L);

            table.increment(inc);

            /**
             * get方法：
             * get方法就相当于增删查改中的查，但是get不想命令行的scan，并不能用多种条件去查找，只能用行键
             * 去查找
             */

            //用行键为row1新建一个get对象：

            Get get = new Get(Bytes.toBytes("row1"));

            //get方法配合Cell接口一起使用

            //先设置Get的MAX_VERSIONS为10：
            get.setMaxVersions(10);

//            HBase会把查询到的结果封装到Result实例中。
//            Result中最常用的方法是getValue(columnFamily, column)
            Result result = table.get(get);

            //用getColumnCells方法获取这个列的多个版本值

//            List<Cell> cells = result.getColumnCells(Bytes.toBytes("mycf"), Bytes.toBytes("name"));
//
//            for(Cell c: cells) {
//                //注意：这里是通过CellUtil.cloneValue来获取数据而不是getValue
//                byte[] cValue = CellUtil.cloneValue(c);
//                System.out.println(Bytes.toString(cValue));
//
//            }
            //我们可以从Result对象中用getValue方法获取到数据， getValue需
            //要的参数是列族（column family） 和列（column）:

//            byte[] name = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name"));
//
//            //使用Hbase API提供的Bytes工具类把byte[]转化为String：
//            System.out.println(Bytes.toString(name));

            /**
             * Scan扫描
             */
            Scan scan = new Scan(Bytes.toBytes("row1"));
            ResultScanner rs = table.getScanner(scan);

            //注意：scan的结果获取本质上跟get不一样， Table通过传入scan
            //之后返回的结果扫描器（ResultScanner） 并不是实际的查询结果。 获
            //取结果扫描器（ResultScanner） 的时候并没有实际地去查询数据。 真
            //正要获取数据的时候要打开扫描器， 然后遍历它， 这个时候才真正地去查询了数据。

            //以下使用for循环的方式来遍历ResultScanner

            for (Result r: rs) {
                String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
                System.out.println(name);
            }
            //这个ResultScanner就像关系型数据库中的ResultSet一样是也是需
            //要持续占用资源的， 所以用完后务必要记得关闭它：
            rs.close();
        }
    }
}
