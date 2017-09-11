import java.util.Comparator;

/**
 * Class for storing document id and score for ranking top k documents. 
 */
public class DocumentScore implements Comparable<DocumentScore> {
  private double score;
  private Long docPosition;

  public DocumentScore(Long docPosition, double score) {
    this.score = score;
    this.docPosition = docPosition;
  }

  @Override
  public int compareTo(DocumentScore score) {
    int result = ((Double)this.score).compareTo(score.getScore());
    if (result == 0) {
      return this.docPosition.compareTo(score.getDocPosition());
    }
    return -1*result;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public Long getDocPosition() {
    return docPosition;
  }

  public void setDocPosition(Long docPosition) {
    this.docPosition = docPosition;
  }
}
