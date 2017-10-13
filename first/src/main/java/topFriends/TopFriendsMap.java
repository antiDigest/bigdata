package topFriends;

import friends.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TopFriendsMap extends Mapper<LongWritable, Text, Pair, Text> {
    private Text friend = new Text();

    /**
     * Map function
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        Pair person;

        String p = next(itr);
        String friends = next(itr);

        if (!friends.equals("None")) {
            String[] fs = friends.split(",");

            for (String f : fs) {
                person = new Pair();
                person.setFirst(p);
                person.setSecond(f);

                friend.set(friends);
                context.write(person, friend);

            }
        }
    }

    /**
     * HELPER FUNCTIONS
     */

    /**
     * Returns next if it exists, else returns "None"
     */
    public String next(StringTokenizer itr) {
        return (itr.hasMoreTokens()) ? itr.nextToken() : "None";
    }


}

