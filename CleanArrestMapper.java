import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanArrestMapper
        extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            //split the input by ','
            String[] lines = value.toString().split(",");
            //Only select id, date, boro, age-group, sex, race and not duplicate records
            int index = lines.length - 19;
            if (!lines[0].equals("ARREST_KEY")){
                context.write(new Text(lines[0] + "," + lines[1] + ","  + lines[8+index] + ","  
                + lines[11+index]+ ","  + lines[12+index]+ "," +lines[13+index]), NullWritable.get());
            }
    }
}