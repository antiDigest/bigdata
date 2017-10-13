package yelpJoin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<Frame, DoubleWritable, Frame, DoubleWritable> {

    int count = 0;
    public void reduce(Frame key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        for(DoubleWritable val:values){
            if(count==10)
                break;
            context.write(key, val);
            count++;
        }
    }
}