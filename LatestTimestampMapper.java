import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Date;

public class LatestTimestampMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    private static final long DAY_IN_MS = 86400000L;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\s+");

        if (parts.length < 3) return;

        int index = Integer.parseInt(parts[0]);

        String docIdStr = parts[1].replaceAll("[^0-9]", "");
        
        long docID;
        try {
            docID = Long.parseLong(docIdStr);
        } catch (NumberFormatException e) {
            return;
        }

        long timestamp = docID * DAY_IN_MS;

        String word = parts[2].replaceAll("[^a-zA-Z]", "");
        context.write(new IntWritable((int) docID), new Text(index + "," + timestamp + "," + word));
    }
}

