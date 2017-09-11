import java.lang.System;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Driver class for Lucene Indexing
 */
public class LuceneDriver {
  public static final String INDEX_DIR = "/luceneoutput";

  public static void main(String[] args) {
    final String cwd = System.getProperty("user.dir");
    final String indexPath = cwd + INDEX_DIR;
    String docPath = cwd + "/input";

    if (args.length > 0) {
      docPath = args[0];
    }

    final Path docDir = Paths.get(docPath);
    if (!Files.isReadable(docDir)) {
      System.out.println("Document directory " + docDir.toAbsolutePath() +
        " does not exist or is not readable.");
      System.exit(1);
    }

    LuceneIndex luceneindex = new LuceneIndex(indexPath, docPath);
    luceneindex.init();
    luceneindex.start();
    luceneindex.finish();
  }
}
