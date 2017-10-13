package inMemoryJoin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InMemoryReduce extends Reducer<Frame, DoubleWritable, Frame, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Frame key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0.0;
        double count = 0.0;

        for(DoubleWritable val: values){
            sum += val.get();
            count++;
        }

        double avg = sum / count ;
        System.out.println(key + "\t" + avg);

        result.set(avg);
        context.write(key, result);
    }
}