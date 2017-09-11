import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

/**
 * Driver class for Hadoop Indexing 
 */
public class HadoopDriver extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new HadoopDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "tweetii");
    FileInputFormat.setInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJar("lib/hdpindex.jar");

    job.setPartitionerClass(TermKeyPartitioner.class);
    job.setGroupingComparatorClass(TermGroupingComparator.class);

    job.setMapOutputKeyClass(TermCompositeKey.class);
    job.setMapOutputValueClass(TermNaturalValue.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(JsonOutputFormat.class);

    job.setMapperClass(HadoopIndex.IndexMapper.class);
    job.setCombinerClass(HadoopIndex.IndexCombiner.class);
    job.setReducerClass(HadoopIndex.IndexReducer.class);
    
    long start = new Date().getTime();
    job.waitForCompletion(true);
    long end = new Date().getTime();
    System.out.println("Hadoop job took " + ((end - start)*0.001) + "seconds");

    return 0;
  }
}
