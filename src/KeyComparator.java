import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class KeyComparator extends WritableComparator{
	protected KeyComparator()
	{
		super(DoubleWritable.class,true);
	}
	@Override
	public int compare(WritableComparable w1,WritableComparable w2){
		int ans = super.compare(w1, w2);
		return -ans;
	}
}
