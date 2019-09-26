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

public class OrderCount {
    public static void main(String[] args) throws Exception {

        Configuration configuration=new Configuration();
        configuration.set("HADOOP_USER_NAME","root");
        FileSystem fs=FileSystem.get(configuration);

        Job job= Job.getInstance(configuration);
        job.setJarByClass(OrderCount.class);
        job.setJobName("OrderCount");



        //要读取文件的路径
        Path input=new Path("/input1/orders.txt");
        FileInputFormat.addInputPath(job,input);

        //结果输出的路径
        Path outpath=new Path("/output");
        if(fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);

        //WordMapper 单词拆分  读入文件一行数据，
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //那些key到同一个reducer
        job.setGroupingComparatorClass(OrderGroupCompartor.class);

        //
        job.setSortComparatorClass(OrderSortCompartor.class);
        //Reducer阶段  处理map阶段输出的结果 <key 1>
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);


        boolean b = job.waitForCompletion(true);
        System.out.println(b);

    }
}

//000001-pdt_01-222.8
class OrderMapper extends Mapper<LongWritable,Text,OrderBean, NullWritable>{
    private OrderBean ob=new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String line=value.toString();
       String[] arrys=line.split("-");
        ob.setItemId(new Text(arrys[0]));
        ob.setAmount(new DoubleWritable(Double.valueOf(arrys[2])));
        context.write(ob,NullWritable.get());
    }
}

//OrderBean[001-300][001-400][001-700]
class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}