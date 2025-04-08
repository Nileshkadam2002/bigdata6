# bigdata6
ass6
Assignment No. 6
Pipeline multiple Map Reduce jobs
1. Understand the Workflow of Multiple Jobs
•	First Job: The first MapReduce job processes the raw data and produces intermediate output.
•	Intermediate Output: The output of this first job is typically stored in HDFS (Hadoop Distributed File System).
•	Subsequent Jobs: The intermediate output of the first job becomes the input for the next job, and so on, until all required operations are complete.
2. Job Dependencies
•	Each job must depend on the output of the previous job. You'll need to manage this dependency and ensure that jobs are executed in the correct order.
•	A common approach is to write a driver program that orchestrates the execution of the MapReduce jobs in sequence.
3. Writing the Driver Code
Here’s a simplified example of how you might write a driver to pipeline two MapReduce jobs in Hadoop:
Example: Pipelining Two Jobs
Job 1: Word Count (Word frequency count)
This first job counts the occurrences of each word in the input text files.
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");
            for (String wordStr : words) {
                word.set(wordStr);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
Job 2: Filter Words with Frequency Greater Than 2
The second job processes the output of the first job to filter and only output words that have a frequency greater than 2
public class FilterWords {
    public static class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable count = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String word = fields[0];
            int wordCount = Integer.parseInt(fields[1]);

            // Output only words with count greater than 10
            if (wordCount > 2) {
                count.set(wordCount);
                context.write(new Text(word), count);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "filter words");
        job.setJarByClass(FilterWords.class);
        job.setMapperClass(FilterMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path from the first job's output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
4. Executing the Jobs in Sequence
The driver logic for running these jobs would look like this:
public class MultiJobDriver {
    public static void main(String[] args) throws Exception {
        // First job (Word Count)
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "word count");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(WordCount.TokenizerMapper.class);
        job1.setReducerClass(WordCount.IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate_output"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);  // Exit if the first job fails
        }

        // Second job (Filter Words with count > 2)
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "filter words");
        job2.setJarByClass(FilterWords.class);
        job2.setMapperClass(FilterWords.FilterMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("intermediate_output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
5. Considerations
•	Output format: The output of each job should be compatible with the input format of the next job. For instance, if the output of Job 1 is a simple key-value pair (word and count), Job 2 should be able to process that format directly.
•	Failure Handling: Always handle potential failures, especially when jobs depend on the output of previous ones. Make sure to check the status of each job before proceeding to the next.
•	Data Format: When piping jobs, ensure that the data format (like Text or SequenceFile) between jobs remains consistent or convert it accordingly.
•	Performance: When chaining multiple MapReduce jobs, be mindful of the intermediate output (stored in HDFS). Consider optimizing the data being written to reduce overhead.


