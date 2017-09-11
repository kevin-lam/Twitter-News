import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.json.JSONException;

public class QueryParserDriver {
  public static void main(String[] args) throws IOException, JSONException {
    QueryParser queryParser = new QueryParser();
    List<Long> list = queryParser.getTopK(25, "trump sucks"); 
    for (long value : list)
      System.out.println(value);
 }
}
