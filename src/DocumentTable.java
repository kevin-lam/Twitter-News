import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for creating the field document file. 
 */
public class DocumentTable {
  private static final String OUTPUT_FILE = "hadoopoutput/tweetinfo";
  private static final String INPUT_DIR = "input";

  private static DocumentTable instance = null;

  TermInfo documentTable = new TermInfo();
  List<TermInfo.DocumentInfo> documents = new ArrayList<>();

  long collectionSize = 0;
  long collectionLength = 0;

  private DocumentTable() {}

  public static synchronized DocumentTable getInstance() {
    if (instance == null) {
      instance = new DocumentTable();
    }
    return instance;
  }

  public void run() throws IOException, JSONException {

    File tweetInfoFile = new File(OUTPUT_FILE);
    if (!tweetInfoFile.exists() && !tweetInfoFile.isDirectory()) {
      tweetInfoFile.getParentFile().mkdirs();
      tweetInfoFile.createNewFile();
    }

    ObjectMapper mapper = new ObjectMapper();
    TextProcessor textProcessor = new TextProcessor();
    File inputDir = new File(INPUT_DIR);
    for (File file : inputDir.listFiles()) {
      if (!file.isDirectory()) {
        try (BufferedReader documentReader = new BufferedReader(new FileReader(file))) {
          String line;
          while ((line = documentReader.readLine()) != null) {
            collectionLength += line.length();
            collectionSize++;
            TermInfo.DocumentInfo documentInfo = new TermInfo.DocumentInfo();
            documentInfo.setDocumentNum(collectionSize);
            documentInfo.setDocumentLength(line.length());

            StringBuilder stringBuilder = new StringBuilder();
            List<String> users = textProcessor.parseUser(line);
            for (String user : users) {
              stringBuilder
                  .append(user)
                  .append(" ");
            }
            documentInfo.setUser(stringBuilder.toString());
            stringBuilder.setLength(0);

            List<String> urls = textProcessor.parseUrl(line);
            for (String url : urls) {
              stringBuilder
                  .append(url)
                  .append(" ");
            }
            documentInfo.setUrl(stringBuilder.toString());
            stringBuilder.setLength(0);

            String noUrl = textProcessor.removeUrl(line);
            String noHashTag = textProcessor.removeHashTag(noUrl);
            String noUser = textProcessor.removeUser(noHashTag);
            String content = noUser;

            documentInfo.setContents(content);
            documents.add(documentInfo);
          }
        }
      }
    }

    long avdl = collectionLength / collectionSize;
    documentTable.setCollectionSize(collectionSize);
    documentTable.setAvdl(avdl);
    documentTable.setDocuments(documents);
    mapper.writeValue(tweetInfoFile, documentTable);
  }
}
