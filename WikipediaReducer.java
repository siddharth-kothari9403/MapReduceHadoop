import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class WikipediaReducer extends Reducer<Text, Text, IntWritable, Text> {

    private IntWritable indexKey = new IntWritable();
    private Text docWordPair = new Text();

    @Override
    protected void reduce(Text docID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> wordList = new ArrayList<>();

        for (Text value : values) {
            wordList.add(value.toString());
        }

        Collections.sort(wordList, Comparator.comparingInt(s -> Integer.parseInt(s.split(",")[0])));
        for (String entry : wordList) {
            String[] parts = entry.split(",", 2);
            int index = Integer.parseInt(parts[0]);
            String word = parts[1];

            indexKey.set(index);
            docWordPair.set("(" + docID.toString() + ", " + word + ")");
            context.write(indexKey, docWordPair);
        }
    }
}

