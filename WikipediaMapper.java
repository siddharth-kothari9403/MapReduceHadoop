import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.StringTokenizer;

public class WikipediaMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text docID = new Text();
    private Text indexWordPair = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Extract document ID (remove ".txt" extension)
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String documentID = fileName.replace(".txt", "");  // Remove ".txt"
        docID.set(documentID);

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int index = 0;

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
            if (!word.isEmpty()) {
                indexWordPair.set(index + "," + word);
                context.write(docID, indexWordPair);
            }
            index++;
        }
    }
}

