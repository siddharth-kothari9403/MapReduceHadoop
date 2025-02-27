import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class LatestTimestampReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private TreeMap<Integer, String> latestDocWords = new TreeMap<>();
    private int maxDocID = Integer.MIN_VALUE;

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int currentDocID = key.get();

        if (currentDocID > maxDocID) {
            maxDocID = currentDocID;
            latestDocWords.clear();
        }

        if (currentDocID == maxDocID) {
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length < 3) continue;

                try {
                    int index = Integer.parseInt(parts[0]);
                    String word = parts[2].trim();

                    latestDocWords.put(index, word);
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> entry : latestDocWords.entrySet()) {
            context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
        }
    }
}

