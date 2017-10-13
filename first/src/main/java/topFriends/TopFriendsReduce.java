package topFriends;

import friends.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopFriendsReduce extends Reducer<Pair, Text, Text, Text> {
    private Text result = new Text();
    private Text keyout = new Text();
    HashMap<String, Frame> hash = new HashMap<>();
    static PriorityQueue<Frame> pq;

    public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> mutualFriends = new LinkedHashSet<>();

        Iterator<Text> val = values.iterator();
//        System.out.println("VALUE : " + val.next().toString());
        mutualFriends.addAll(Arrays.asList(val.next().toString().split(",")));

        while (val.hasNext()) {
            List<String> fs = Arrays.asList(val.next().toString().split(","));
            mutualFriends.retainAll(fs);
        }

        int count = mutualFriends.size();
        Frame frame = new Frame();
        if (count > 0) {
            frame.setFirst(key.getFirst());
            frame.setSecond(key.getSecond());
            frame.setCount(count);
//            System.out.println(frame + ": " + frame.getCount());
            hash.put(frame.toString(), frame);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        pq = new PriorityQueue<>(10);

        for (Frame frame : hash.values()) {
//            System.out.println(frame + ": " + frame.getCount());
            if (pq.isEmpty()) {
                pq.add(frame);
            } else if (pq.size() < 10) {
                pq.add(frame);
            } else if (frame.compareTo(pq.peek()) > 0) {
                pq.remove();
                pq.add(frame);
            }
        }

        for (int i = 0; i < 10; i++) {
            Frame frame = pq.remove();
//            System.out.println(frame + ": " + frame.getCount());
            keyout.set(frame.toString());
            result.set(frame.getCount() + "");
            context.write(keyout, result);
        }
    }
}

