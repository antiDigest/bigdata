package friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class Friends {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("USAGE: jar <input-file> <output-file>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:54310"), conf);

        hdfs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf, "Friends");
        job.setJarByClass(Friends.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FriendMap.class);
        job.setReducerClass(FriendReduce.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (success)
            System.exit(0);
        else
            System.exit(-1);
    }
}