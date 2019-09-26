import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private Text itemId;
    private DoubleWritable amount;

    public Text getItemId() {
        return itemId;
    }

    public void setItemId(Text itemId) {
        this.itemId = itemId;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    public void setAmount(DoubleWritable amount) {
        this.amount = amount;
    }

    public int compareTo(OrderBean o) {
        int ret=this.itemId.compareTo(o.itemId);
        if(ret==0){
            return -this.amount.compareTo(o.amount);
        }
        return ret;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.itemId.toString());
        dataOutput.writeDouble(this.amount.get());

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.itemId=new Text(dataInput.readUTF());
        this.amount=new DoubleWritable(dataInput.readDouble());
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "itemId=" + itemId.toString() +
                ", amount=" + amount.get() +
                '}';
    }
}
