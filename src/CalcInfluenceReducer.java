import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
	
public class CalcInfluenceReducer 
       extends Reducer<Text,Text,DoubleWritable,Text> {
	
    private int[] RetweetCount = new int[7];
    private int[] CommentCount = new int[7];
    private final static String[] Range = new String[7];
    private int cou = 0;
    private double[] CommentWeight =new double[7];
    
    CalcInfluenceReducer(){
    	super();
    	Range[0]="Zero" ;
        Range[1]="OneHud" ;
        Range[2]="FiveHud" ;
        Range[3]="OneThos" ;
        Range[4]="ThreeThos" ;
        Range[5]="FiveThos" ;
        Range[6]="TheThos" ;
        
        for(int i=0; i<7; i++)
        	CommentWeight[i]=(i+1)*0.2;
    }

    public void init(){
    	for(int i = 0;i<7;i++){
    		RetweetCount[i]=0;
    		CommentCount[i]=0;
    	}
    }
    
    public void count(String val){
    	String[] value = val.split(" ");
    	cou++;
    	for(int i = 0;i<7;i++)
    	{
    		if(Range[i].equals(value[0]))
    			RetweetCount[i]++;
    		
    		if(Range[i].equals(value[1]))
    			CommentCount[i]++;
    	}
    }
    
    public double InfluValue(int num){
    	double ret = 0;
    	int totalrt=0;
    	for(int i = 0;i<7;i++)
    	{
    		totalrt+=RetweetCount[i];
    		if ( i == 0 ) 
    			ret += (double)(CommentCount[i]) * CommentWeight[i]; 
   
    		else 
    			ret += (double)(RetweetCount[i]) * (1<<(i-1)) + (double)(CommentCount[i]) * CommentWeight[i];
    	}
    	
    	return 100*totalrt/num + ret*100.0/cou;
    }
    
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      init();
      cou = 0;
      for (Text val : values) 
        count(val.toString());
      
      String[] keyStr = key.toString().split("@");
      String name = keyStr[0];
      int followerNum = Integer.parseInt(keyStr[1]);
      double InfluVal = InfluValue(followerNum);
      
      context.write( new DoubleWritable(InfluVal),new Text(name));
    }
}
