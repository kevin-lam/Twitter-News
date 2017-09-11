import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Composite key for grouping terms and document id prior to the combine phase
 * of Mapreduce. 
 */
public class TermCompositeKey implements WritableComparable<TermCompositeKey> {

  private String term;
  private Long docPosition;
  private Integer tf;

  public TermCompositeKey() {}

  public TermCompositeKey(String term, Long docPosition, Integer tf) {
    this.term = term;
    this.docPosition = docPosition;
    this.tf = tf;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, term);
    dataOutput.writeLong(docPosition);
    dataOutput.writeInt(tf);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    term = WritableUtils.readString(dataInput);
    docPosition = dataInput.readLong();
    tf = dataInput.readInt();
  }

  @Override
  public int compareTo(TermCompositeKey termCompositeKey) {
    int result = this.term.compareTo(termCompositeKey.getTerm());
    if (result == 0) {
      	result = this.tf.compareTo(termCompositeKey.getTf());
      if (result == 0) {
        return this.docPosition.compareTo(termCompositeKey.getDocPosition());
      }
      return -1*result;
    }
    return result;
  }

  @Override
  public String toString() {
    return this.term;
  }

  public String getTerm() {
    return term;
  }

  public void setTerm(String term) {
    this.term = term;
  }

  public long getDocPosition() {
    return docPosition;
  }

  public void setDocPosition(long docPosition) {
    this.docPosition = docPosition;
  }

  public int getTf() {
    return tf;
  }

  public void setTf(int tf) {
    this.tf = tf;
  }
}
