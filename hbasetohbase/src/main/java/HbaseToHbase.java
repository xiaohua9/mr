import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

public class HbaseToHbase {
    public static void main(String[] args) throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("fs.defaultFS","hdfs://hadoop2:8020");
        configuration.set("hbase.zookeeper.quorum", "hadoop2,hadoop3,hadoop4");
        Job job= Job.getInstance(configuration,"HbaseToHbase");
        job.setJarByClass(HbaseToHbase.class);

        Scan scan=new Scan();
        TableMapReduceUtil.initTableMapperJob("student",scan,H2HMapper.class,ImmutableBytesWritable.class,Put.class,job);

        job.setOutputValueClass(Put.class);
        job.setOutputKeyClass(NullWritable.class);
        TableMapReduceUtil.initTableReducerJob("new_student",H2HReducer.class,job,null,null,null,null,false);
        boolean wait = job.waitForCompletion(true);
        System.out.println(wait);
    }
}

class H2HMapper extends TableMapper<ImmutableBytesWritable,Put>{
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put=new Put(key.get());
        List<Cell> cells = value.listCells();
        for (Cell cell:cells){
            if ("cf".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                put.add(cell);
            }
        }
        context.write(key,put);
    }
}
class H2HReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put:values){
            context.write(NullWritable.get(),put);
        }
    }
}

