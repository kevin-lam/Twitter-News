import java.util.List;

/**
 * POJO class which stores the JSON Object which represents a posting in the 
 * hadoop inverted index. 
 */
public class Posting implements Comparable<String> {
  private String term;
  private long occurrence;
  private List<PostingEntry> posting;
  
  public Posting(){}
  
  public Posting(String term, long occurrence, List<PostingEntry> posting) {
    this.term = term;
    this.occurrence = occurrence;
    this.posting = posting;
  }  

  @Override
  public int compareTo(String term) {
    return this.term.compareTo(term);
  }

  public String getTerm() {
    return term;
  }

  public void setTerm(String term) {
    this.term = term;
  }

  public long getOccurrence() {
    return occurrence;
  }

  public void setOccurrence(long occurrence) {
    this.occurrence = occurrence;
  }

  public List<PostingEntry> getPosting() {
    return posting;
  }

  public void setPosting(List<PostingEntry> posting) {
    this.posting = posting;
  }

  public static class PostingEntry {
    public long documentNum;
    public String positions;
    public int frequency;

    public PostingEntry(){}

    public PostingEntry(long documentNum, String positions, int frequency) {
      this.documentNum = documentNum;
      this.positions = positions;
      this.frequency = frequency;
    }

    public long getDocumentNum() {
      return documentNum;
    }

    public void setDocumentNum(long documentNum) {
      this.documentNum = documentNum;
    }

    public String getPositions() {
      return positions;
    }

    public void setPositions(String positions) {
      this.positions = positions;
    }

    public int getFrequency() {
      return frequency;
    }

    public void setFrequency(int frequency) {
      this.frequency = frequency;
    }
  }
}
