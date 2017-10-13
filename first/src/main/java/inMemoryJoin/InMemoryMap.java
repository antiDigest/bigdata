package inMemoryJoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class InMemoryMap extends Mapper<LongWritable, Text, Frame, DoubleWritable> {

    private DoubleWritable valueout = new DoubleWritable();
    private HashMap<String, Frame> hash = new HashMap<>();
    int count = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();

        try {
            BufferedReader readBuffer1 = new BufferedReader(new FileReader(new Path(uris[0]).getName()));
            String line;
            while ((line = readBuffer1.readLine()) != null) {
                Frame keyout = new Frame();
                String[] values = line.split("::");

                keyout.setId(values[0]);

                if (values[1].toLowerCase().contains("Palo Alto".toLowerCase())) {
                    hash.put(keyout.getId(), keyout);
                }
            }
            readBuffer1.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    /**
     * Map function
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("::");

        String id = values[2];
        Frame keyout = hash.get(id);

        if (keyout != null) {
//            System.out.println("COUNT: " + count++ );
            keyout.setUserId(values[1]);
            keyout.setRating(Double.parseDouble(values[3]));
            System.out.println(keyout + "\t" + keyout.getRating());
            valueout.set(Double.parseDouble(values[3]));
            context.write(keyout, valueout);
        }
    }

}