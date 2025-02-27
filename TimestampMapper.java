import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\s+");

        System.out.println("Line: " + line);
        System.out.println("Parts length: " + parts.length);

        if (parts.length < 3) {
            System.out.println("Skipping line due to insufficient parts.");
            return;
        }

        int index = Integer.parseInt(parts[0]);
        String docIdStr = parts[1].replaceAll("[^0-9]", "");
        
	    long timestamp;
        try {
            long daysSinceEpoch = Long.parseLong(docIdStr);
            timestamp = daysSinceEpoch * 86400000L;
            Date date = new Date(timestamp);
        } catch (NumberFormatException e) {
            timestamp = -1;
            System.out.println("Conversion Unsuccessful for docId: " + docIdStr);
        }

	    String word = parts[2].replaceAll("[^a-zA-Z]", "");
        System.out.println("Index: " + index + ", Timestamp: " + timestamp + ", Word: " + word);
        context.write(new IntWritable(index), new Text(timestamp + "," + word));
    }
}

