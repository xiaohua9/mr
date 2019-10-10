
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

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration= HBaseConfiguration.create();
        configuration.set("HADOOP_USER_NAME","root");
        configuration.set("fs.defaultFS","hdfs://hadoop1:8020");
        configuration.set("hbase.zookeeper.quorum", "hadoop2,hadoop3,hadoop4");
        Job job= Job.getInstance(configuration);
        FileSystem fs=FileSystem.get(configuration);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        job.setMapperClass(WordMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        Path inpath=new Path("/input");
        FileInputFormat.addInputPath(job,inpath);

        job.setOutputValueClass(Put.class);
        job.setOutputKeyClass(NullWritable.class);
        TableMapReduceUtil.initTableReducerJob("wordcount",WordReducer.class,job,null,null,null,null,false);
        boolean wait = job.waitForCompletion(true);
        System.out.println(wait);


    }
}


class WordMap extends Mapper<LongWritable, Text,Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] arrays=line.split(" ");
        for(String str:arrays){
            context.write(new Text(str),new IntWritable(1));
        }
    }
}

class WordReducer extends TableReducer<Text,IntWritable, ImmutableBytesWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable nu:values){
            count+=nu.get();
        }
        Put put=new Put(Bytes.toBytes(key.toString()));
        put.addColumn("cf".getBytes(),"num".getBytes(),String.valueOf(count).getBytes());
        context.write(null,put);
    }
}