import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\s+");  // Splitting on whitespace

        // Debugging: Print extracted parts
        // System.out.println("Line: " + line);
        // System.out.println("Parts length: " + parts.length);

        if (parts.length < 3) {
            System.out.println("Skipping line due to insufficient parts.");
            return;  // Ensure correct format
        }

        int index = Integer.parseInt(parts[0]);

        // Extract docID and remove surrounding characters like '(' and ','
        String docIdStr = parts[1].replaceAll("[^0-9]", "");  // Keep only digits
        
	long timestamp;
        try {
            // Convert doc ID to long
            long daysSinceEpoch = Long.parseLong(docIdStr);

            // Convert days to milliseconds (1 day = 86400000 ms)
            timestamp = daysSinceEpoch * 86400000L;

            // Convert to Java Date format
            Date date = new Date(timestamp);
            // System.out.println("Converted Timestamp: " + timestamp);
            // System.out.println("Date Equivalent: " + date);
        } catch (NumberFormatException e) {
            timestamp = -1;
            System.out.println("Conversion Unsuccessful for docId: " + docIdStr);
        }

	String word = parts[2].replaceAll("[^a-zA-Z]", "");

        // Debugging: Print extracted values
        // System.out.println("Index: " + index + ", Timestamp: " + timestamp + ", Word: " + word);

        // Emit (index, "docID,word")
        context.write(new IntWritable(index), new Text(timestamp + "," + word));
    }
}

