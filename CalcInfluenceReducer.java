import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
	
public class CalcInfluenceReducer 
       extends Reducer<Text,Text,IntWritable,Text> {
	
    private int[] RetweetCount = new int[7];
    private int[] CommentCount = new int[7];
    private final static String[] Range = new String[7];
    
    CalcInfluenceReducer(){
    	super();
    	Range[0]="Zero" ;
        Range[1]="OneHud" ;
        Range[2]="FiveHud" ;
        Range[3]="OneThos" ;
        Range[4]="ThreeThos" ;
        Range[5]="FiveThos" ;
        Range[6]="TheThos" ;
    }

    public void init(){
    	for(int i = 0;i<7;i++){
    		RetweetCount[i]=0;
    		CommentCount[i]=0;
    	}
    }
    
    public void count(String val){
    	String[] value = val.split(" ");
    	for(int i = 0;i<7;i++)
    	{
    		if(Range[i].equals(value[0]))
    			RetweetCount[i]++;
    		
    		if(Range[i].equals(value[1]))
    			CommentCount[i]++;
    	}
    }
    
    public int InfluValue(int num){
    	int ret = 0;
//    	for(int i = 0;i<7;i++)
//    	ret += double(RetweetCount[i]) * (1<<(i-1)) / num + double(CommentCount[i]) * i * double(rand()%100) 
//    			 / ( num * 1000 );
    	for(int i = 0;i<7;i++)
    		ret=ret+(i+1)*RetweetCount[i]+(i+1)*CommentCount[i];
    	
    	return ret;
    }
    
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      init();
      
      for (Text val : values) 
        count(val.toString());
      
      String[] keyStr = key.toString().split("@");
      String name = keyStr[0];
      int followerNum = Integer.parseInt(keyStr[1]);
      int InfluVal = InfluValue(followerNum);
      
      context.write( new IntWritable(InfluVal),new Text(name));
    }
}
