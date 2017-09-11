import org.json.JSONException;

import java.io.IOException;

/**
 * Driver class for creating the field document file. 
 */
public class TableDriver {
  public static void main(String[] args) throws IOException, JSONException {
    DocumentTable.getInstance().run();
  }
}
