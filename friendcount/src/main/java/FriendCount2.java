import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendCount2 {
    public static void main(String[] args)  throws Exception{
        Configuration configuration=new Configuration();
        configuration.set("HADOOP_USER_NAME","root");
        FileSystem fs=FileSystem.get(configuration);

        Job job= Job.getInstance(configuration);
        job.setJarByClass(FriendCount2.class);
        job.setJobName("FriendCount2");



        //要读取文件的路径
        Path input=new Path("/output");
        FileInputFormat.addInputPath(job,input);

        //结果输出的路径
        Path outpath=new Path("/output1");
        if(fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);

        //WordMapper 单词拆分  读入文件一行数据，
        job.setMapperClass(Friend2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Reducer阶段  处理map阶段输出的结果 <key 1>
        job.setReducerClass(Friend2Reducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        boolean b = job.waitForCompletion(true);
        System.out.println(b);

    }
}


class Friend2Mapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] arrs=line.split("\t");
        context.write(new Text(arrs[0]),new Text(arrs[1]));
    }
}


class Friend2Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer=new StringBuffer("");
        for(Text text:values){
            buffer.append(text.toString()).append(",");
        }
        context.write(key,new Text(buffer.toString()));
    }
}
