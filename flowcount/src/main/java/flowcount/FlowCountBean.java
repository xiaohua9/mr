package flowcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowCountBean implements Writable {
    //上传流量，下载流量，总流量
    private Text upFlow;
    private Text dFlow;
    private Text sumFlow;

    public FlowCountBean() {
    }

    public FlowCountBean(Text upFlow, Text dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow=new Text((Integer.parseInt(upFlow.toString())+Integer.parseInt(upFlow.toString()))+"");
    }

    public Text getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Text upFlow) {
        this.upFlow = upFlow;
    }

    public Text getdFlow() {
        return dFlow;
    }

    public void setdFlow(Text dFlow) {
        this.dFlow = dFlow;
    }

    public Text getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Text sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.upFlow.toString());
            dataOutput.writeUTF(this.dFlow.toString());
            dataOutput.writeUTF(this.sumFlow.toString());
    }

    public void readFields(DataInput dataInput) throws IOException {
            this.upFlow=new Text(dataInput.readUTF());
            this.dFlow=new Text(dataInput.readUTF());
            this.sumFlow=new Text(dataInput.readUTF());

    }

    @Override
    public String toString() {
        return "FlowCountBean{" +
                "upFlow=" + upFlow.toString() +
                ", dFlow=" + dFlow.toString() +
                ", sumFlow=" + sumFlow.toString() +
                '}';
    }
}
