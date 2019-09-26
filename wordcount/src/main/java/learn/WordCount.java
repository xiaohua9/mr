package learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        configuration.set("HADOOP_USER_NAME","root");
        FileSystem fs=FileSystem.get(configuration);

        Job job= Job.getInstance(configuration);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        //要读取文件的路径
        Path input=new Path("/input");
        FileInputFormat.addInputPath(job,input);

        //结果输出的路径
        Path outpath=new Path("/output");
        if(fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);

        //WordMapper 单词拆分  读入文件一行数据，
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Reducer阶段  处理map阶段输出的结果 <key 1>
        job.setReducerClass(WordReducer.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);

        boolean b = job.waitForCompletion(true);
        System.out.println(b);


    }
}

class WordMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    //文件有多少行，map方法就执行多少次，每次读文件中一行数据
    //oracle javaee admin tom
    //<orace 1><javaee 1><admin 1> <tom 1>
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] arrs=line.split(" ");
        for(String str:arrs){
            context.write(new Text(str),new IntWritable(1));
        }
    }
}

class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable num: values){
            count=count+num.get();
        }
        context.write(key,new IntWritable(count));
    }
}
