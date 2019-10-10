import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MoveToHbase {
    public static void main(String[] args) throws Exception{
        Configuration configuration= HBaseConfiguration.create();
        configuration.set("HADOOP_USER_NAME","root");
        configuration.set("fs.defaultFS","hdfs://hadoop1:8020");
        configuration.set("hbase.zookeeper.quorum", "hadoop2,hadoop3,hadoop4");
        Job job= Job.getInstance(configuration);
        FileSystem fs=FileSystem.get(configuration);
        job.setJarByClass(MoveToHbase.class);
        job.setJobName("MoveToHbase");

        job.setMapperClass(MoveToHbaseMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        Path inpath=new Path("/hdfsdatatohbase/hdfstohbase");
        FileInputFormat.addInputPath(job,inpath);

        job.setOutputValueClass(Put.class);
        job.setOutputKeyClass(NullWritable.class);
        TableMapReduceUtil.initTableReducerJob("human",MoveToHbaseReducer.class,job,null,null,null,null,false);
        boolean wait = job.waitForCompletion(true);
        System.out.println(wait);


    }
}


class MoveToHbaseMap extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] arrays=line.split(",");
        for(String str:arrays){
            if (str.equals(arrays[0])){
                continue;
            }
            context.write(new Text(arrays[0]),new Text(str));
        }
    }
}

class MoveToHbaseReducer extends TableReducer<Text,Text, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Put put=new Put(Bytes.toBytes(key.toString()));
        int i=0;
        for(Text v:values){
            if (i==0) {
                put.addColumn("cf".getBytes(),"name".getBytes(),v.toString().getBytes());
                i++;
                continue;
            }
            if (i==1){
                put.addColumn("cf".getBytes(),"age".getBytes(),v.toString().getBytes());
                i++;
                continue;
            }
            if (i==2){
                put.addColumn("cf".getBytes(),"place".getBytes(),v.toString().getBytes());
                i++;
                continue;
            }
        }
        context.write(null,put);
    }
}