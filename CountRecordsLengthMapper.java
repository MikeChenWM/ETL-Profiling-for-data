import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CountRecordsLengthMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] lines = value.toString().split(",");
        context.write(new Text("Total length of records is " + Integer.toString(lines.length) + " in Arrest file: "), new IntWritable(1));
    }
}