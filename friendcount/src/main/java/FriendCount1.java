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
import java.util.Arrays;

public class FriendCount1 {
    public static void main(String[] args)  throws Exception{
        Configuration configuration=new Configuration();
        configuration.set("HADOOP_USER_NAME","root");
        FileSystem fs=FileSystem.get(configuration);

        Job job= Job.getInstance(configuration);
        job.setJarByClass(FriendCount1.class);
        job.setJobName("FriendCount1");



        //要读取文件的路径
        Path input=new Path("/input1/friend.txt");
        FileInputFormat.addInputPath(job,input);

        //结果输出的路径
        Path outpath=new Path("/output");
        if(fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);


        //job.setGroupingComparatorClass(xxxx.class);
        //WordMapper 单词拆分  读入文件一行数据，
        job.setMapperClass(Friend1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Reducer阶段  处理map阶段输出的结果 <key 1>
        job.setReducerClass(Friend1Reducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        boolean b = job.waitForCompletion(true);
        System.out.println(b);

    }
}

//A:B,C,D,F,E,O
class Friend1Mapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields =line.split(":");
        String[] arrs=fields[1].split(",");
        for(String str:arrs){
            context.write(new Text(str),new Text(fields[0]));
        }

    }
}
//<B A><C A><D A><F A><E A><O A>
class Friend1Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer=new StringBuffer("");
        for(Text text:values){
            buffer.append(text.toString()).append(",");
        }
        String str=buffer.toString().substring(0,buffer.toString().lastIndexOf(","));
        String[] arrs=str.split(",");
        Arrays.sort(arrs);
        for(int i=0;i<arrs.length-1;i++){
            for(int j=i+1;j<arrs.length;j++){
                String k1=arrs[i]+"-"+arrs[j];
                context.write(new Text(k1),key);
            }
        }

        //buffer中存放的是key 的好友
        //A B,C,E,B
        // context.write(key,new Text(buffer.toString()));
    }
}
