package learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class HbaseTest {
    private Connection connection;
    private Admin admin;
    @Before
    public void TestInit() throws Exception{
        Configuration configuration=HBaseConfiguration.create();//获得hbase的配置对象
        configuration.set("hbase.zookeeper.quorum","hadoop2,hadoop3,hadoop4");//指定zookeeper集群
        configuration.set("HADOOP_USER_NAME","root");//以root用户登录hadoop
        connection=ConnectionFactory.createConnection(configuration);//获取连接
    }
    @Test
    public void testCreateTable() throws Exception{
        Admin admin = connection.getAdmin();//表管理者
        TableName tableName=TableName.valueOf("student");
        if (admin.tableExists(tableName)){//如果表存在就把表删除
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor desc=new HTableDescriptor(tableName);//表描述
        HColumnDescriptor family=new HColumnDescriptor("cf".getBytes());//列族
        desc.addFamily(family);//将列族添加到表结构
        admin.createTable(desc);//创建表

    }

    @Test
    public void addDataTest() throws Exception{
        TableName tableName=TableName.valueOf("student");//要插数据的表名
        Table table = connection.getTable(tableName);//要查数据的表
        Put put=new Put("103".getBytes());//指定添加的rowkey
        put.addColumn("cf".getBytes(),"name".getBytes(),"wangwu".getBytes());
        put.addColumn("cf".getBytes(),"age".getBytes(),"23".getBytes());
        table.put(put);
    }
    @Test
    public void deleteDataTest() throws Exception{
        TableName tableName=TableName.valueOf("student");
        Table table = connection.getTable(tableName);
        Delete delete=new Delete("101".getBytes());//指定删除的rowkey
        delete.addColumn("cf".getBytes(),"name".getBytes());//删除name列，不指定的话就删除所有
        table.delete(delete);
    }
    @Test//指定rowkey的查询
    public void getData() throws Exception{
        TableName tableName=TableName.valueOf("student");
        Table table = connection.getTable(tableName);
        Get get=new Get("102".getBytes());//获取指定rowkey的信息
        //get.addColumn("cf".getBytes(),"name".getBytes());
        Result result = table.get(get);
        String name=Bytes.toString(result.getValue("cf".getBytes(),"name".getBytes()));
        String age=Bytes.toString(result.getValue("cf".getBytes(),"age".getBytes()));
        System.out.println(name+","+age);

    }
    @Test//扫描方式/带条件
    public void scanData() throws Exception{
        TableName tableName=TableName.valueOf("student");
        Table table = connection.getTable(tableName);
        Scan scan=new Scan();

        /*指定开始和结束的条件*//*
        scan.setStartRow("101".getBytes());
        scan.setStopRow("103".getBytes());*/

        /*//指定过滤条件(单条件)
        SingleColumnValueFilter scvf= new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,"lisi".getBytes());
        scvf.setFilterIfMissing(true); //默认为false， 没有此列的数据也会返回 ，为true则只返回name=lisi的数据
        scan.setFilter(scvf);*/
/////////多条件过滤
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);//or
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("cf".getBytes(),"name".getBytes(), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("lisi"));
        list.addFilter(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf".getBytes(),"age".getBytes(), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("23"));
        list.addFilter(filter2);
        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result:scanner){
            String rowKey = Bytes.toString(result.getRow());
            List<Cell> cells = result.listCells();
            for (Cell cell:cells){
                String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowKey+","+columnFamily+","+column+","+value);
            }
        }
    }
}
