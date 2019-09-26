import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortCompartor extends WritableComparator {
    public OrderSortCompartor() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1=(OrderBean)a;
        OrderBean o2=(OrderBean)b;
        int ret=o1.getItemId().compareTo(o2.getItemId());
        if(ret==0){
            return -o1.getAmount().compareTo(o2.getAmount());
        }
        return ret;
    }
}
