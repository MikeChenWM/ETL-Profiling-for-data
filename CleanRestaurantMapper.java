import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanRestaurantMapper
        extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            //split the records by ','
            String[] lines = value.toString().split(",");
            //store result only with id, boro, cuisine, grade
            int i = 0;
            String boro = "No boro";
            //store the boro
            for (String word: lines){
                if (word.equals("Bronx") || word.equals("Brooklyn") || word.equals("Queens")|| word.equals("Staten Island")|| word.equals("Manhattan")) {
                    boro = word;
                    break;
                }
            }

            //store the cuisine
            String cuisine = "No cuisine";
            cuisine = lines[7];    

            //store the grade
            String grade = "No grade";
            for (String word: lines){
                if (word.equals("A")){
                    grade = "A";
                    break;
                }else if (word.equals("B")){
                    grade = "B";
                    break;
                }else if (word.equals("C")){
                    grade = "C";
                    break;
                }
            }
            if (!lines[0].equals("CAMIS")){
                context.write(new Text(lines[0] + "," + boro + "," + cuisine + "," + grade), NullWritable.get());         
            }
    }
}