import java.util.List;

/**
 * POJO class that stores the JSON Object from the field document file in Java
 * object form. 
 */
public class TermInfo {
  private long avdl;
  private List<DocumentInfo> documents;
  private long collectionSize;
  
  public TermInfo() {}

  public TermInfo(long avdl, List<DocumentInfo> documents, long collectionSize){
    this.avdl = avdl;
    this.documents = documents;
    this.collectionSize = collectionSize;
  }
  

  public long getAvdl() {
    return avdl;
  }

  public void setAvdl(long avdl) {
    this.avdl = avdl;
  }

  public List<DocumentInfo> getDocuments() {
    return documents;
  }

  public void setDocuments(List<DocumentInfo> documents) {
    this.documents = documents;
  }

  public long getCollectionSize() {
    return collectionSize;
  }

  public void setCollectionSize(long collectionSize) {
    this.collectionSize = collectionSize;
  }

  public static class DocumentInfo {
    private long documentLength;
    private long documentNum;
    private String contents;
    private String user;
    private String url;

    public DocumentInfo() {
    }

    public DocumentInfo(long documentLength, long documentNum, String contents, String user, String url) {
      this.documentLength = documentLength;
      this.documentNum = documentNum;
      this.contents = contents;
      this.user = user;
      this.url = url;
    }

    public long getDocumentLength() {
      return documentLength;
    }

    public void setDocumentLength(long documentLength) {
      this.documentLength = documentLength;
    }

    public long getDocumentNum() {
      return documentNum;
    }

    public void setDocumentNum(long documentNum) {
      this.documentNum = documentNum;
    }

    public String getContents() {
      return contents;
    }

    public void setContents(String contents) {
      this.contents = contents;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }
  }
}
