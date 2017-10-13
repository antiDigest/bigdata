package yelpJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMap extends Mapper<LongWritable, Text, Text, Text> {

    private Text valueout = new Text();

    /**
     * Map function
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("::");

        if (values.length == 4) {
            reviewMap(values, context);
        } else {
            if (values[2].matches("http://.*")) {
                userMap(values, context);
            } else {
                businessMap(values, context);
            }
        }

    }

    private void reviewMap(String[] values, Context context) throws IOException, InterruptedException {
        valueout.set(values[3]);
        Text keyout = new Text();
        keyout.set(values[2]);
        context.write(keyout, valueout);
    }

    private void userMap(String[] values, Context context) {
        return;
    }

    private void businessMap(String[] values, Context context) throws IOException, InterruptedException {
        valueout.set(values[1] + "\t" + values[2]);
        Text keyout = new Text();
        keyout.set(values[0]);
        context.write(keyout, valueout);
    }

}