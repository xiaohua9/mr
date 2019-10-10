
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PhoneTest {
    private Admin admin;
    private Connection conn;
    private String name = "tbl_phone";
    private TableName tableName;
    private Table table;
    private Random random = new Random();

    @Before
    public void before() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("HADOOP_USER_NAME","root");
        configuration.set("fs.defaultFS","hdfs://hadoop1:8020");
        configuration.set("hbase.zookeeper.quorum", "hadoop2,hadoop3,hadoop4");
        conn = ConnectionFactory.createConnection(configuration);
        admin = conn.getAdmin();
        tableName = TableName.valueOf(name);
        table = conn.getTable(tableName);
    }

    /**
     * 如果tbl_phone表不存在就创建
     *
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor column = new HColumnDescriptor("cf".getBytes());
        tableDescriptor.addFamily(column);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(tableDescriptor);
        System.out.println(name + "表创建成功!!");
    }

    /**
     * 10个用户，每个用户每年产生1000条通话记录
     * dnum:对方手机号 type:类型：0主叫，1被叫 length：通话时长 date:时间
     *
     * @throws Exception
     */
   SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    @Test
    public void initTableData() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++) {
            String phoneNumber = getPhone("158");
            for (int j = 0; j < 1000; j++) {
                String dnum = getPhone("177");
                String length = String.valueOf(random.nextInt(99));
                String type = String.valueOf(random.nextInt(2));
                String date = getDate("2018");
                String rowKey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
                Put put = new Put(rowKey.getBytes());
                put.addColumn("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.addColumn("cf".getBytes(), "length".getBytes(), length.getBytes());
                put.addColumn("cf".getBytes(), "type".getBytes(), type.getBytes());
                put.addColumn("cf".getBytes(), "date".getBytes(), date.getBytes());
                puts.add(put);
            }
            table.put(puts);
        }
    }

    //随机生成一个日期
    private String getDate(String s) {
        String date = s + String.format("%02d%02d%02d%02d%02d", random.nextInt(11), random.nextInt(31), random.nextInt(12), random.nextInt(60), random.nextInt(60));
        return date;
    }

    private String getPhone(String s) {

        String phone = s + String.format("%08d", random.nextInt(999));
        return phone;
    }

    /**
     * 查询某一个用户3月份的所有通话记录 条件： 1、某一个电话 2、时间4月到5月间的通话
     *
     * @throws Exception
     */
    @Test
    public void findData() throws Exception {
        String phone = "15800000121";
        String startKey = "15800000121_" + String.valueOf(Long.MAX_VALUE - sdf.parse("20180500000000").getTime());
        String endKey = "15800000121_" + String.valueOf(Long.MAX_VALUE - sdf.parse("20180600000000").getTime());
        Scan scan = new Scan();
        scan.setStartRow(startKey.getBytes());
        //scan.setStopRow(endKey.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String dnum = getFamilyColumnValue(result, "cf", "dnum");
            String length = getFamilyColumnValue(result, "cf", "length");
            String type = getFamilyColumnValue(result, "cf", "type");
            String date = getFamilyColumnValue(result, "cf", "date");
            System.out.println(phone + "," + dnum + "," + length + "," + type + "," + date);
        }
    }

    private String getFamilyColumnValue(Result result, String family, String column) {
        return Bytes.toString(result.getValue(family.getBytes(), column.getBytes()));
    }


    /**
     * 查询某一个用户。所有的主叫电话 条件： 1、电话号码 2、type=0
     *
     * @throws Exception
     */

    @Test
    public void findDataByCondiation() throws Exception {
        String phone = "15800000121";
        //MUST_PASS_ONE 与 MUST_PASS_ALL是or与and的关系
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(), CompareFilter.CompareOp.EQUAL, "0".getBytes());
       // PrefixFilter filter2 = new PrefixFilter(phone.getBytes());
       filters.addFilter(filter1);
        //filters.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(filters);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            String dnum = getFamilyColumnValue(result, "cf", "dnum");
            String length = getFamilyColumnValue(result, "cf", "length");
            String type = getFamilyColumnValue(result, "cf", "type");
            String date = getFamilyColumnValue(result, "cf", "date");
            System.out.println(rowKey + "," + phone + "," + dnum + "," + length + "," + type + "," + date);
        }
    }

    @Test
    public void findDataByRowKey() throws Exception {
        String rowKey = "15800000121_9223370524872940807";
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        if (result != null) {
            String dnum = getFamilyColumnValue(result, "cf", "dnum");
            String length = getFamilyColumnValue(result, "cf", "length");
            String type = getFamilyColumnValue(result, "cf", "type");
            String date = getFamilyColumnValue(result, "cf", "date");
            System.out.println(rowKey + "," + dnum + "," + length + "," + type + "," + date);

        }
    }
    @After
    public void destory() throws Exception {

        if (admin != null) {
            admin.close();
        }
    }


}
