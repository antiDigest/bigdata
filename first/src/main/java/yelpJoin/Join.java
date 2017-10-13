package yelpJoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import topFriends.TopFriendsReduce;

import java.io.IOException;
import java.net.URI;

public class Join {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("USAGE: jar <input-folder> <output-folder>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:54310"), conf);

        hdfs.delete(new Path("/user/user/intermediate_out"), true);
        hdfs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf, "Reduce Side Join job 1");
        job.setJarByClass(Join.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(JoinMap.class);
        job.setReducerClass(JoinReduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("/user/user/intermediate_out"));

        boolean success = job.waitForCompletion(true);
        if (!success)
            System.exit(-1);

        Job job2 = Job.getInstance(conf, "Reduce Side Join job 2");
        job2.setJarByClass(Join.class);

        job2.setOutputKeyClass(Frame.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.setMapperClass(SortMap.class);
        job2.setReducerClass(SortReduce.class);

        FileInputFormat.addInputPath(job2, new Path("/user/user/intermediate_out"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        success = job2.waitForCompletion(true);
        if (success)
            System.exit(0);
        else
            System.exit(-1);
    }
}