import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class which handles processing of document and query text. 
 */
public class TextProcessor {
  public TextProcessor() {}

  public String removePunctuation(String text) {
    Pattern removePunctuationPattern = Pattern.compile
        ("(?<=\\s)[^\\w\\s]+|[^\\w\\s]+\\s+|\\s+[^\\w\\s]+|[^\\w\\s]?\\s+|[^\\w\\s]+$}");
    Matcher matcher = removePunctuationPattern.matcher(text);
    return matcher.replaceAll(" ");
  }

  public String removeStopWords(String text) {
    Pattern removeStopWordsPattern = Pattern.compile("\\b" +
        "(a|an|and|are|as|at|be|but|by|for|if|in|into|is|it|no|not|of|on|or|such|that|the|their" +
        "|then|there|these|they|this|to|was|will|with)\\b");
    Matcher matcher = removeStopWordsPattern.matcher(text);
    return matcher.replaceAll("");
  }

  public List<String> parseUser(String text) {
    List<String> users = new ArrayList<>();
    Pattern usersPattern = Pattern.compile("@\\w+");
    Matcher matcher = usersPattern.matcher(text);
    while (matcher.find()) {
      users.add(matcher.group().substring(1));
    }
    return users;
  }

  public String removeUser(String text) {
    String temp = text;
    Pattern userPattern = Pattern.compile("^RT.*?:\\s");
    Matcher matcher = userPattern.matcher(temp);
    temp = matcher.replaceFirst("");
    return temp;
  }

  public List<String> parseUrl(String text) {
    List<String> urls = new ArrayList<>();
    Pattern urlPattern = Pattern.compile("https:\\/\\/.+?\\s?$");
    Matcher matcher = urlPattern.matcher(text);
    while (matcher.find()) {
      urls.add(matcher.group());
    }
    return urls;
  }

  public String removeUrl(String text) {
    String temp = text;
    Pattern urlPattern = Pattern.compile("https:\\/\\/.+?\\s?$");
    Matcher matcher = urlPattern.matcher(temp);
    temp = matcher.replaceAll("");
    return temp;
  }

  public String removeHashTag(String text) {
    String temp = text;
    Pattern hashTagPattern = Pattern.compile("#");
    Matcher matcher = hashTagPattern.matcher(temp);
    temp = matcher.replaceAll("");
    return temp;
  }
}
