package flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        configuration.set("HADOOP_USER_NAME","root");
        FileSystem fs=FileSystem.get(configuration);

        Job job= Job.getInstance(configuration);
        job.setJarByClass(FlowCount.class);
        job.setJobName("FlowCount");



        //要读取文件的路径
        Path input=new Path("/input1/flowinfo.txt");
            FileInputFormat.addInputPath(job,input);

        //结果输出的路径
        Path outpath=new Path("/output");
            if(fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);

        //WordMapper 单词拆分  读入文件一行数据，
            job.setMapperClass(FlowMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowCountBean .class);

        //Reducer阶段  处理map阶段输出的结果 <key 1>
            job.setReducerClass(FlowReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowCountBean.class);


        boolean b = job.waitForCompletion(true);
            System.out.println(b);

    }
}

//000001-pdt_01-222.8
class FlowMapper extends Mapper<LongWritable,Text,Text, FlowCountBean> {
    private FlowCountBean flowCountBean=new FlowCountBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] arrys=line.split(" ");

        flowCountBean.setUpFlow(new Text(arrys[1]));
        flowCountBean.setdFlow(new Text(arrys[2]));
        flowCountBean.setSumFlow(new Text(Integer.parseInt(new Text(arrys[1]).toString())+Integer.parseInt(new Text(arrys[2]).toString())+""));
        context.write(new Text(arrys[0]),flowCountBean);
    }
}

class FlowReducer extends Reducer<Text,FlowCountBean,Text,FlowCountBean> {
    private FlowCountBean flowCountBean=new FlowCountBean();
    @Override
    protected void reduce(Text key, Iterable<FlowCountBean> values, Context context) throws IOException, InterruptedException {
        int tmpUpFlow=0;
        int tmpDFlow=0;
        int tmpSumFlow=0;
        for (FlowCountBean f:values){
            tmpUpFlow+=Integer.parseInt(f.getUpFlow().toString());
            tmpDFlow+=Integer.parseInt(f.getdFlow().toString());
            tmpSumFlow+=Integer.parseInt(f.getSumFlow().toString());
        }
        flowCountBean.setUpFlow(new Text(tmpUpFlow+""));
        flowCountBean.setdFlow(new Text(tmpDFlow+""));
        flowCountBean.setSumFlow(new Text(tmpSumFlow+""));
        context.write(key,flowCountBean);
    }
}
