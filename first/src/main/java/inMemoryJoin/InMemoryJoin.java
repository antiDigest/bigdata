package inMemoryJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import yelpJoin.JoinReduce;
import yelpJoin.SortMap;
import yelpJoin.SortReduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class InMemoryJoin {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        if (args.length < 3) {
            System.err.println("USAGE: jar <business-file> <review-file> <output-folder>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:54310"), conf);

        hdfs.delete(new Path(args[2]), true);

        Job job = Job.getInstance(conf, "In Memory Join");
        job.setJarByClass(InMemoryJoin.class);

        job.setOutputKeyClass(Frame.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(InMemoryMap.class);
        job.setReducerClass(InMemoryReduce.class);

        job.addCacheFile(new Path(args[0]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        if (success)
            System.exit(0);
        else
            System.exit(-1);
    }
}