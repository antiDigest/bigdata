package friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class FriendReduce extends Reducer<Pair, Text, Text, Text> {
    private Text result = new Text();
    private Text keyout = new Text();
    List<Pair> outputs = new ArrayList<>();
    HashMap<String, String> hash = new HashMap<>();

    public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> mutualFriends = new LinkedHashSet<>();

        Iterator<Text> val = values.iterator();
        mutualFriends.addAll(Arrays.asList(val.next().toString().split(",")));

        while (val.hasNext()) {
            List<String> fs = Arrays.asList(val.next().toString().split(","));
            mutualFriends.retainAll(fs);
        }

//        int count = mutualFriends.size();
        key.setSecond(key.getSecond());
        hash.put(key.toString(), mutualFriends.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.add(new Pair("0", "4"));
        outputs.add(new Pair("20", "22939"));
        outputs.add(new Pair("1", "29826"));
        outputs.add(new Pair("6222", "19272"));
        outputs.add(new Pair("28041", "28056"));

        for (Pair output : outputs) {
            keyout.set(output.toString());
            String res = hash.get(output.toString());
            if (res != null) {
                result.set(res);
                context.write(keyout, result);
            }
        }
    }
}

