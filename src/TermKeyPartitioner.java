import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner class for Mapreduce which assigns keys to nodes. 
 */
public class TermKeyPartitioner extends Partitioner<TermCompositeKey, IntWritable> {
  @Override
  public int getPartition(TermCompositeKey termCompositeKey, IntWritable intWritable, int numPartitions) {
    int hash = termCompositeKey.getTerm().hashCode();
    int partition = hash % numPartitions;
    return partition;
  }
}
