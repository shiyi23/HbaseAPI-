package org.huang.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 十分钟入门级版本
 */
//public class HelloHbase {
////    public static void main(String[] args) throws URISyntaxException{
////        Configuration config = HBaseConfiguration.create();
////        config.addResource(
////        new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()) );
////
////        config.addResource(
////        new Path(ClassLoader.getSystemResource("core-site.xml").toURI()) );
////
////        config.addResource(
////        new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()) );
////
////        try ( Connection connection = ConnectionFactory.createConnection(config)){
////
////            Admin admin = connection.getAdmin();
////            TableName tableName = TableName.valueOf("hsy_table");
////            HTableDescriptor table = new HTableDescriptor(tableName);
////            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
////            //设置mycf这个列族的压缩方式为GZ
////            mycf.setCompactionCompressionType(Compression.Algorithm.GZ);
////            //把最大版本数修改为ALL_VERSIONS， ALL_VERSIONS的值其实
////            //就是Integer.MAX_VALUE
////            mycf.setMaxVersions(HConstants.ALL_VERSIONS);
////            table.modifyFamily(mycf);//把列族的定义更新到表定义里面去
////            admin.modifyTable(tableName,table);
////            table.addFamily(new HColumnDescriptor(mycf));
////
////            /**
////             * 往已经存在的表里添加列族操作示例（以mytable为例）
////             **/
////            HColumnDescriptor newColumn = new HColumnDescriptor("hsy_newcf");
////            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
////            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
////            admin.addColumn(tableName,newColumn);
////
////            //在建表之前判断表是否已经存在与数据库中，避免
////            //            表数据被覆盖而清空；
////            if(admin.tableExists(table.getTableName())){
////                admin.disableTable(table.getTableName());
////                admin.deleteTable(table.getTableName());
////            }
////            admin.createTable(table);
////
////            admin.close();
////            connection.close();
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
////    }
////}

/**
 * 三十分钟高阶教程版本
 */
public class HelloHbase {
    /**
     * 先检查hsy_table2这张表是否存在再建立它
     * @param admin
     * @param table
     * @throws IOException
     */
    public static void createOrOverwrite(Admin admin, HTableDescriptor table)
            throws IOException {
        if (admin.tableExists(table.getTableName())) {
        admin.disableTable(table.getTableName());
        admin.deleteTable(table.getTableName());
        }
    admin.createTable(table);
    }

    /**
     * 建立hsy_mytable2表
     *
     * @throws IOException
     */
    public static void createSchemaTables(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("hsy_table2"));
            table.addFamily(new HColumnDescriptor("mycf").setCompactionCompressionType(Algorithm.NONE));

            System.out.print("新建表：");
            createOrOverwrite(admin,table);
            System.out.println("建表完毕");
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void modifySchema(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
                 Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("hsy_table2");

            if(!admin.tableExists(tableName)) {
                System.out.println("该表不存在");
                System.exit(-1);
            }

            //往hsy_table2里添加名为“newcf”的列族
            HColumnDescriptor newColumn = new HColumnDescriptor("newcf");
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            //获取表定义
            HTableDescriptor table = admin.getTableDescriptor(tableName);

            //更新mycy这个列族
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            mycf.setCompactionCompressionType(Algorithm.GZ);
            mycf.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(mycf);
            admin.modifyTable(tableName,table);

            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), Bytes.toBytes("rose"));

        }

    }


    public static void main(String[] args) throws URISyntaxException,IOException {

        Configuration config = HBaseConfiguration.create();

        //添加必要的配置文件: hbase-site.xml、core-site.xml、hdfs-site.xml
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));

        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));

        //建表
        createSchemaTables(config);

        //改表
        modifySchema(config);

    }

}






















