package redis.seek;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.codec.language.Metaphone;

public class Text {
    private static final Pattern delimiter = Pattern
            .compile("[\\s|,|\\.\\s|_|\\-|\\(|\\)|\\\"]");
    private Metaphone encoder = new Metaphone();
    private String text;

    public Text(String text) {
        this.text = text;
    }

    public Set<String> getWords() {
        String[] split = delimiter.split(text);
        Set<String> words = new HashSet<String>();
        for (String word : split) {
            String metaphone = encoder.encode(word);
            if (metaphone.isEmpty()) {
                metaphone = word;
            }

            words.add(metaphone);
        }
        /*
         * Set<String> words = new HashSet<String>(); Scanner s = new
         * Scanner(text); s.useDelimiter(delimiter); while (s.hasNext()) {
         * String word = s.next(); // TODO: use stop words here? String
         * metaphone = encoder.encode(word); if (metaphone.isEmpty()) {
         * metaphone = word; }
         * 
         * words.add(metaphone); }
         */

        return words;
    }

}
