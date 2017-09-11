import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import jdk.nashorn.internal.parser.JSONParser;

/**
 * Class that handles the ranking and querying process for the hadoop inverted
 * index. 
 */
public class QueryParser {

  private static final String INVERTED_INDEX_FILE = "hadoopoutput/tweetii";
  private static final String TERM_INFO_FILE = "hadoopoutput/tweetinfo";

  TermInfo iiTermInfo;

  public QueryParser() throws IOException, JSONException {
    iiTermInfo = readTermInfoFromJson();
  }

  public List<String> getTopKUsers(int k, String query) throws IOException, JSONException {
    List<String> listTopKUsers = new ArrayList<>();
    List<Long> listTopK = getTopK(k, query);
    for (Long docPosition : listTopK) {
      listTopKUsers.add(iiTermInfo.getDocuments().get((int)(docPosition - 1)).getUser());
    }
    return listTopKUsers;
  }

  public List<String> getTopKDocuments(int k, String query) throws IOException, JSONException {
    List<String> listTopKDocuments = new ArrayList<>();
    List<Long> listTopK = getTopK(k, query);
    for (Long docPosition : listTopK) {
      listTopKDocuments.add(iiTermInfo.getDocuments().get((int)(docPosition - 1)).getContents());
    }
    return listTopKDocuments;
  }

  public List<String> getTopKUrls(int k, String query) throws IOException, JSONException {
    List<String> listTopKUrls = new ArrayList<String>();
    List<Long> listTopK = getTopK(k, query);
    for (Long docPosition : listTopK) {
      listTopKUrls.add(iiTermInfo.getDocuments().get((int)(docPosition - 1)).getUrl());
    }
    return listTopKUrls;
  }

  public List<Long> getTopK(int k, String query) throws IOException, JSONException {

    // Process query removing unnecessary words and punctuation
    String preProcessedQuery = query;
    TextProcessor textProcessor = new TextProcessor();
    String noPunctuation = textProcessor.removePunctuation(preProcessedQuery);
    String noStopWords = textProcessor.removeStopWords(noPunctuation);
    String postProcessedQuery = noStopWords;
    PorterStemmer porterStemmer = new PorterStemmer();

    StringTokenizer iterator = new StringTokenizer(postProcessedQuery.toLowerCase());

    // Separate query into individual terms
    List<String> queryTerms = new ArrayList<>();
    while (iterator.hasMoreTokens()) {
      queryTerms.add(porterStemmer.stripAffixes(iterator.nextToken()));
    }

    // Term-at-a-time: getting the query term postings
    List<Posting> matchingPosting = new ArrayList<>();
    for (String term : queryTerms) {
      Posting posting = getPostingByTerm(term);
      if (posting != null) {
        matchingPosting.add(posting);
      }
    }

    // HashMap to store frequency of query terms
    Map<String, Integer> queryTermFrequency = new HashMap<>();
    for (String term : queryTerms) {
      if (!queryTermFrequency.containsKey(term)) {
        queryTermFrequency.put(term, 1);
      } else {
        int frequency = queryTermFrequency.get(term);
        queryTermFrequency.put(term, ++frequency);
      }
    }

    // Compute scores on one term
    Map<Long, Double> mapPartialScore = new HashMap<>();
    for (Posting posting : matchingPosting) {
      String term = posting.getTerm();
      double ni = posting.getOccurrence();
      for (Posting.PostingEntry entry : posting.getPosting()) {
        long docPosition = entry.getDocumentNum();
        double fi = entry.getFrequency();
        double k1 = 1.2;
        double k2 = 100;
        double b = 0.75;
        double N = iiTermInfo.getCollectionSize();
        double avdl = iiTermInfo.getAvdl();
        double dl = iiTermInfo.getDocuments().get((int)(docPosition - 1)).getDocumentLength();
        double K = k1 * ((1-b)+(b*(dl/avdl)));
        double qfi = queryTermFrequency.get(term);
        double ri = 0;
        double R = 0;
        double newPartialScore = partialBm25Similarity(k1, k2, b, N, avdl, dl, ni,
            K, fi, qfi, ri, R);
        if (!mapPartialScore.containsKey(docPosition)) {
          mapPartialScore.put(docPosition, newPartialScore);
        } else {
          double partialScore = mapPartialScore.get(docPosition);
          mapPartialScore.put(docPosition, partialScore + newPartialScore);
        }
      }
    }

    // Pair score and documents. Then sort to get the highest score
    List<DocumentScore> listDocumentScore = new ArrayList<>();
    for (Map.Entry<Long, Double> scoreEntry : mapPartialScore.entrySet()) {
      long docPosition = scoreEntry.getKey();
      double score = scoreEntry.getValue();
      listDocumentScore.add(new DocumentScore(docPosition, score));
    }
    Collections.sort(listDocumentScore);

    int size = listDocumentScore.size() < k ? listDocumentScore.size() : k;
    List<Long> listTopK = new ArrayList<>();
    for (int index=0; index < size; index++) {
      DocumentScore documentScore = listDocumentScore.get(index);
      listTopK.add(documentScore.getDocPosition());
    }
    return listTopK;
  }

  public double partialBm25Similarity(double k1, double k2, double b, double N,
                                      double avdl, double dl, double ni, double K, double fi,
                                      double qfi, double ri, double R) throws
      IOException, JSONException {

    double bm25Front = (Math.log(((ri+0.5)/(R-ri+0.5))*((N-ni-R+ri+0.5)/(ni-ri+0.5))))/(Math.log(2));
    double bm25Mid = ((k1+1)*fi)/(K+fi);
    double bm25End = ((k2+1)*qfi)/(k2+qfi);
    return (bm25Front*bm25Mid*bm25End);
  }

  private synchronized Posting getPostingByTerm(String term) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.setCodec(mapper);
    JsonParser parser = jsonFactory.createParser(new File(INVERTED_INDEX_FILE));

    JsonToken current = parser.nextToken();
    if (current != JsonToken.START_OBJECT) {
      return null;
    }

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = parser.getCurrentName();
      current = parser.nextToken();
      if (fieldName.equals("terms")){
        if (current == JsonToken.START_ARRAY) {
          while (parser.nextToken() != JsonToken.END_ARRAY) {
            JsonNode node = parser.readValueAsTree();
            List<String> termlist = new ArrayList<>();
            if (node.has("term")) {
              termlist = node.findValuesAsText("term");
            }
            if (termlist.size() > 0 && termlist.get(0).equals(term)) {
              return mapper.treeToValue(node, Posting.class);
            }  
          }
        } else {
          parser.skipChildren();
        }
      } else {
        parser.skipChildren();
      }
    }
    return null;
  }

  private synchronized TermInfo readTermInfoFromJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(TERM_INFO_FILE), TermInfo.class);
  }
}
