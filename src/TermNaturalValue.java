import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Class for storing the values of each document. 
 */
public class TermNaturalValue implements WritableComparable<TermNaturalValue>, Comparator<TermNaturalValue> {

  private Long docPosition;
  private Integer tf;
  private String termPositionList;

  public TermNaturalValue() {}

  public TermNaturalValue(long docPosition, int tf, String termPositionList) {
    this.docPosition = docPosition;
    this.tf = tf;
    this.termPositionList = termPositionList;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(docPosition);
    dataOutput.writeInt(tf);
    WritableUtils.writeString(dataOutput, termPositionList);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    docPosition = dataInput.readLong();
    tf = dataInput.readInt();
    termPositionList = WritableUtils.readString(dataInput);
  }

  @Override
  public int compareTo(TermNaturalValue termNaturalValue) {
    return -1*(this.tf.compareTo(termNaturalValue.getTf()));
  }


  @Override
  public int compare(TermNaturalValue termNaturalValue1, TermNaturalValue termNaturalValue2) {
    int result = 0;
    result = ((Integer)termNaturalValue1.getTf()).compareTo(termNaturalValue2.getTf());
    if (result == 0) {
      return ((Long)termNaturalValue1.getDocPosition()).compareTo(termNaturalValue2
          .getDocPosition());
    }
    return -1*result;
  }

  @Override
  public String toString() {
    return (docPosition + "(" + tf + ")");
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

  public String getTermPositionList() {
    return termPositionList;
  }

  public void setTermPositionList(String termPositionList) {
    this.termPositionList = termPositionList;
  }
}
