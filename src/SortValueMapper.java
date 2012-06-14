import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
	
public class SortValueMapper 
       extends Mapper<LongWritable, Text, LongWritable,Text>{
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] val = value.toString().split("\t");
        context.write(new LongWritable(Long.parseLong(val[0])), new Text(val[1]));
      
    }
 }
  
  