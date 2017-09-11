import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator class for grouping terms up prior to the reduce phase of 
 * Mapreduce. 
 */
public class TermGroupingComparator extends WritableComparator {
  protected TermGroupingComparator() {
    super(TermCompositeKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2) {
    TermCompositeKey termCompositeKey1 = (TermCompositeKey) wc1;
    TermCompositeKey termCompositeKey2 = (TermCompositeKey) wc2;

    return termCompositeKey1.getTerm().compareTo(termCompositeKey2.getTerm());
  }
}
