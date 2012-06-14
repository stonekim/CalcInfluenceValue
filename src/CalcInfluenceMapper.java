import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
	
public class CalcInfluenceMapper 
       extends Mapper<LongWritable, Text, Text,Text>{
    
    private final static String[] Range = new String[7];
    
    public CalcInfluenceMapper(){
    	super();
    	Range[0]="Zero" ;
        Range[1]="OneHud" ;
        Range[2]="FiveHud" ;
        Range[3]="OneThos" ;
        Range[4]="ThreeThos" ;
        Range[5]="FiveThos" ;
        Range[6]="TheThos" ;
    }
    
    public String getRange(int value){
    	if(value<=100)
    		return Range[0];
    	else if (value>100&&value<=500)
    		return Range[1];
    	else if (value>500&&value<=1000)
    		return Range[2];
    	else if (value>1000&&value<=3000)
    		return Range[3];
    	else if (value>3000&&value<=5000)
    		return Range[4];
    	else if (value>5000&&value<=10000)
    		return Range[5];
    	else return Range[6];
    }
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] val = value.toString().split(" ");
      Text word = new Text();
      word.set(getRange(Integer.parseInt(val[1]))+" "+getRange(Integer.parseInt(val[2])));
        context.write(new Text(val[0]), word);
      
    }
 }
  
  