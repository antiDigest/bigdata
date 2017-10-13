package yelpJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import topFriends.Frame;

import java.io.IOException;

public class JoinReduce extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String output = "";
        double sum = 0.0;
        double count = 0.0;

        for(Text val: values){
            if(!val.toString().matches(".*\\t+.*")){
                sum += Double.parseDouble(val.toString());
                count++;
            }
            else{
                output = val.toString();
            }
        }

        double avg = sum / count ;

        result.set(output + "\t" + avg);
        context.write(key, result);
    }

}