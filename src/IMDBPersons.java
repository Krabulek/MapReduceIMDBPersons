import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class IMDBPersons extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(IMDBPersons.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new IMDBPersons(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "imdbpersons");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IMDBPersonsMapper.class);
        job.setCombinerClass(IMDBPersonsReducer.class);
        job.setReducerClass(IMDBPersonsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumJobs.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class IMDBPersonsMapper extends Mapper<LongWritable, Text, Text, SumJobs> {

        private Text personId = new Text();
        private SumJobs sumJobs = new SumJobs();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                if (key.get() == 0)
                    return;
                else {
                    String line = value.toString();
                    //testLine.set(line);
                    int i = 0;
                    for (String word : line
                            .split("\t")) {
                        if (i == 2) {
                            personId.set(word);
                        }
                        if (i == 3) {
                            if (word.equalsIgnoreCase("director")) {
                                sumJobs.set(new IntWritable(1), new IntWritable(0));
                            }
                            else if (word.equalsIgnoreCase("self")
                                    || word.equalsIgnoreCase("actor")
                                    || word.equalsIgnoreCase("actress")) {
                                sumJobs.set(new IntWritable(0), new IntWritable(1));
                            }
                            else {
                                return;
                            }
                        }
                        i++;
                    }
                    context.write(personId, sumJobs);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class IMDBPersonsReducer extends Reducer<Text, SumJobs, Text, SumJobs> {
        private SumJobs result = new SumJobs();

        @Override
        public void reduce(Text key, Iterable<SumJobs> values,
                           Context context) throws IOException, InterruptedException {
            SumJobs sumJobs = new SumJobs();
            for (SumJobs job : values) {
                sumJobs.addSumJobs(job);
            }
            result.set(sumJobs.getNumDirected(), sumJobs.getNumPlayed());
            context.write(key, result);
        }
    }
}
