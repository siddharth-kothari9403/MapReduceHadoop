import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Date;

public class LatestTimestampMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    private static final long DAY_IN_MS = 86400000L; // 1 day in milliseconds

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\s+"); // Split on whitespace

        if (parts.length < 3) return; // Ensure correct format

        int index = Integer.parseInt(parts[0]);

        // Extract docID and remove non-digit characters
        String docIdStr = parts[1].replaceAll("[^0-9]", "");
        
        long docID;
        try {
            docID = Long.parseLong(docIdStr);
        } catch (NumberFormatException e) {
            return; // Skip invalid doc IDs
        }

        // Convert docID (days since epoch) to timestamp (milliseconds)
        long timestamp = docID * DAY_IN_MS;

        // Extract and clean the word
        String word = parts[2].replaceAll("[^a-zA-Z]", "");

        // Emit (docID, "index,timestamp,word") as key-value
        context.write(new IntWritable((int) docID), new Text(index + "," + timestamp + "," + word));
    }
}

