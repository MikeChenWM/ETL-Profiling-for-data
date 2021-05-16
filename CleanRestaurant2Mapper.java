import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanRestaurant2Mapper
        extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            //split the records by ','
            String[] lines = value.toString().split(",");

            if (!lines[1].equals("No boro") && !lines[2].equals("No cuisine") && !lines[3].equals("No grade")) {
                try{
                    Long i = Long.parseLong(lines[2]);
                } catch (Exception e){
                    context.write(new Text(value), NullWritable.get());         
                }
            }          
    }
}