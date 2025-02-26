import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MaxTimestampReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
	System.out.println("Key - "+key.toString());
	String latestWord = "";
        long latestTimestamp = Long.MIN_VALUE;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length < 2) continue;  // Expecting timestamp and word

            try {
                // Extract timestamp from mapper output
                long timestamp = Long.parseLong(parts[0]);  // Already converted in mapper
                
                // Extract word and clean up unwanted characters
                String word = parts[1].replaceAll("[^a-zA-Z]", "").trim();

                // Keep the latest (max) timestamp's word
                if (timestamp > latestTimestamp) {
                    latestTimestamp = timestamp;
                    latestWord = word;
                }
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid line: " + val.toString());
            }
        }

        // Emit (index, latestWord)
        context.write(key, new Text(latestWord));
    }
}

