import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Driver class for Lucene querying. 
 */
public class SearchDriver {
  public static void main(String[] args) {
    SearchIndex searchIndex = new SearchIndex();
    QueryParser queryParser = searchIndex.init();

    String userQuery = null;
    BufferedReader input = new BufferedReader(new InputStreamReader(
        System.in, StandardCharsets.UTF_8));
    while (true) {
      System.out.println("What are you looking for? ");
      try {
        while (userQuery == null || userQuery.length() == -1
            || userQuery.length() == 0 || userQuery.trim().length() == 0) {
          userQuery = input.readLine();
        }
        if (userQuery.equals("EOF")) {
          break;
        }
        Query query = queryParser.parse(userQuery);
        String parsedQuery = "";
        if (query != null) {
          parsedQuery = query.toString(searchIndex.getField());
        }
        System.out.println("Searching for " + userQuery);
        String suggestions = searchIndex.spellcheck(parsedQuery);

        if (suggestions.equals(parsedQuery)) {
          searchIndex.search(query, input);
        } else {
          System.out.println("Auto-correcting to " + suggestions);
          searchIndex.search(queryParser.parse(suggestions), input);
        }
        userQuery = "";
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } catch (ParseException pe) {
        pe.printStackTrace();
      }
    }
    try {
      input.close();
      searchIndex.finish();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
