import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.Runnable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer.Builder;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Class that creates the lucene index
 */
public class LuceneIndex {

  private String indexPath, docPath;
  private Similarity rankingAlg;
  private IndexWriter indexWriter;

  public LuceneIndex () {

  }

  public LuceneIndex (String indexPath, String docPath) {
    this.indexPath = indexPath;
    this.docPath = docPath;
    this.rankingAlg = null;
    this.indexWriter = null;
  }

  public void init() {
    try {
      Directory dir = FSDirectory.open(Paths.get(indexPath));
      // TEXT ANALYSIS: tokenizes lowercase, remove stop words, split words at
      // puncutation. Trims beginning and end whitespace. Algorithmic stemming
      // of words.
      Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("classic")
          .addTokenFilter("lowercase")
          .addTokenFilter("stop")
          .addTokenFilter("trim")
          .addTokenFilter("kstem")
          .build();

      // WRITER CONFIG: sets configuration of indexwriter: multithreading,
      // BM25, creating/appending new index files, and using compound files.
      IndexWriterConfig iwconfig = new IndexWriterConfig(analyzer);
      iwconfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
      iwconfig.setCommitOnClose(true);
      ConcurrentMergeScheduler mergescheduler = new ConcurrentMergeScheduler();
      iwconfig.setMergeScheduler(mergescheduler);
      this.rankingAlg = new BM25Similarity();
      iwconfig.setSimilarity(rankingAlg);
      iwconfig.setUseCompoundFile(true);

      // Initialize a new index writer
      this.indexWriter = new IndexWriter(dir, iwconfig);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public void start() {
    try {
      Files.walkFileTree(Paths.get(docPath), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(final Path file, BasicFileAttributes attrs) {
          try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file.toFile()))) {
            String tweet;
            int count = 0;
            long startTime = System.nanoTime();
            while ((tweet = bufferedReader.readLine()) != null && tweet.length() != 0) {
              addDocument(indexWriter, tweet);
              count++;
            }
            System.out.println("Finished!!!!");
            long endTime = System.nanoTime();
            System.out.printf("Lucene indexing %d documents took %.6f seconds.\n", count, (
                (endTime - startTime) / 1000000000.000000f));
            bufferedReader.close();
          } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
          } catch (IOException ioe) {
            ioe.printStackTrace();
          }
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void finish() {
    try {
      indexWriter.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public void addDocument(IndexWriter writer, String tweet) {
    Pattern pattern;
    Matcher matcher;

    Document document = new Document();

    // Get all user names starting with @ ie. @CNN
    pattern = Pattern.compile("@\\w+");
    matcher = pattern.matcher(tweet);
    while (matcher.find()) {
      document.add(new StringField("user", matcher.group().substring(1), Field.Store.YES));
    }

    // Get the url
    pattern = Pattern.compile("https:\\/\\/.+?\\s?$");
    matcher = pattern.matcher(tweet);
    while (matcher.find()) {
      document.add(new StringField("url", matcher.group(), Field.Store.YES));
      tweet = matcher.replaceFirst("");
    }

    // Remove all hashtags
    pattern = Pattern.compile("#");
    matcher = pattern.matcher(tweet);
    tweet = matcher.replaceAll("");

    // Get the content by removing all the user names in the beginning of the tweet
    pattern = Pattern.compile("^RT.*?:\\s");
    matcher = pattern.matcher(tweet);
    tweet = matcher.replaceFirst("");
    document.add(new TextField("contents", tweet, Field.Store.YES));
    try {
      writer.addDocument(document);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
