package yelpJoin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMap extends Mapper<LongWritable, Text, Frame, DoubleWritable> {

    private DoubleWritable valueout = new DoubleWritable();
    private Frame keyout = new Frame();

    /**
     * Map function
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\t+");

        keyout.setId(values[0]);
        keyout.setAddress(values[1]);
        keyout.setList(values[2]);
        keyout.setRating(Double.parseDouble(values[3]));

        valueout.set(Double.parseDouble(values[3]));

        context.write(keyout, valueout);
    }

}