import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupCompartor extends WritableComparator {
    public OrderGroupCompartor() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1=(OrderBean)a;
        OrderBean o2=(OrderBean)b;
        return o1.getItemId().compareTo(o2.getItemId());
    }

}
