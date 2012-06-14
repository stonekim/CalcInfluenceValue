

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalcInfluenceValue {


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: CalcInfluenceValue <in> <out> <sortout>");
      System.exit(2);
    }
    Job job = new Job(conf, "Calculate Influence Value");
    Job sortJob = new Job(conf,"Sort value");
    
    job.setJarByClass(CalcInfluenceValue.class);
    job.setMapperClass(CalcInfluenceMapper.class);
    job.setReducerClass(CalcInfluenceReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    if(job.waitForCompletion(true)){
    sortJob.setJarByClass(CalcInfluenceValue.class);
    sortJob.setMapperClass(SortValueMapper.class);
    sortJob.setReducerClass(Reducer.class);
    sortJob.setOutputKeyClass(LongWritable.class);
    sortJob.setOutputValueClass(Text.class);
    sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
    TextInputFormat.addInputPath(sortJob, new Path(otherArgs[1]+"/part-r-00000"));
    FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[2]));
    }
    System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
  }
}
