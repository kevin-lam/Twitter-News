import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Class for map, combine, and reduce for Hadoop. 
 */
public class HadoopIndex {

  public static class IndexMapper extends Mapper<LongWritable, Text, TermCompositeKey,
      TermNaturalValue> {

    private long docPosition = 0;
    private TextProcessor textAnalyzer = new TextProcessor();

    protected void map(LongWritable key, Text text, Context context) throws IOException,
        InterruptedException {
      long termPosition = 0;

      String noPunctuation = textAnalyzer.removePunctuation(text.toString());
      String noStopWords = textAnalyzer.removeStopWords(noPunctuation);
      String line = noStopWords;
      StringTokenizer iterator = new StringTokenizer(line.toLowerCase());
      PorterStemmer porterStemmer = new PorterStemmer();
      docPosition++;

      while (iterator.hasMoreTokens()) {
        termPosition++;
        String term = porterStemmer.stripAffixes(iterator.nextToken());
        TermCompositeKey termCompositeKey = new TermCompositeKey(term, docPosition, 1);
        TermNaturalValue termNaturalValue = new TermNaturalValue(docPosition, 1, Long.toString
            (termPosition));
        context.write(termCompositeKey, termNaturalValue);
      }
    }
  }

  public static class IndexCombiner extends Reducer<TermCompositeKey, TermNaturalValue, TermCompositeKey,
      TermNaturalValue> {

    protected void reduce(TermCompositeKey termCompositeKey, Iterable<TermNaturalValue> iterable, Context
        context) throws
        IOException, InterruptedException {
      int sum = 0;
      StringBuilder positionList = new StringBuilder();
      for (TermNaturalValue termNaturalValue : iterable) {
        sum++;
        positionList
            .append(termNaturalValue.getTermPositionList())
            .append(" ");
      }
      termCompositeKey.setTf(sum);
      context.write(termCompositeKey, new TermNaturalValue(termCompositeKey.getDocPosition(),
          sum, positionList.toString()));
    }
  }

  public static class IndexReducer extends Reducer<TermCompositeKey, TermNaturalValue, Text,
      NullWritable> {

    protected void reduce(TermCompositeKey termCompositeKey, Iterable<TermNaturalValue> iterable, Context
        context) throws
        IOException, InterruptedException {
      ArrayList<TermNaturalValue> termNaturalValueList = new ArrayList<>();

      for (TermNaturalValue termNaturalValue : iterable) {
        Long docPosition = termNaturalValue.getDocPosition();
        Integer tf = termNaturalValue.getTf();
        String positions = termNaturalValue.getTermPositionList();
        termNaturalValueList.add(new TermNaturalValue(docPosition, tf, positions));
      }

      Collections.sort(termNaturalValueList);

      Text jsonTermString = convertPostingToJSON(termCompositeKey, termNaturalValueList);

      context.write(jsonTermString, null);
    }
  }

  protected static Text convertPostingToJSON(TermCompositeKey key, ArrayList<TermNaturalValue>
      values) throws JsonProcessingException {

    ObjectMapper mapper = new ObjectMapper();
    Posting entry = new Posting();
    entry.setTerm(key.getTerm());
    entry.setOccurrence(values.size());
    List<Posting.PostingEntry> posting = new ArrayList<>();
    for (TermNaturalValue value : values) {
      Posting.PostingEntry termInfo = new Posting.PostingEntry();
      Long docPosition = value.getDocPosition();
      Integer tf = value.getTf();
      String positions = value.getTermPositionList();
      String sortedPositions = sortPositions(positions);

      termInfo.setDocumentNum(docPosition);
      termInfo.setFrequency(tf);
      termInfo.setPositions(sortedPositions);
      posting.add(termInfo);
    }
    entry.setPosting(posting);
    return new Text(mapper.writeValueAsString(entry));
  }

  protected static String sortPositions(String positions) {

    List<Long> positionList = new ArrayList<>();
    StringTokenizer stringTokenizer =  new StringTokenizer(positions, " ");
    while (stringTokenizer.hasMoreTokens()) {
      positionList.add(Long.parseLong(stringTokenizer.nextToken()));
    }
    Collections.sort(positionList);
    StringBuilder stringBuilder = new StringBuilder();
    for (Long position : positionList) {
      stringBuilder
          .append(Long.toString(position))
          .append(" ");
    }
    return stringBuilder.toString();
  }
}
