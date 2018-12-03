package ex2;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 4.2 Exercise 2 – Term co-occurrences
 * In the following exercise, we need to build the term co-occurrence matrix for a text collection. A co-occurrence matrix
 * is a n x n matrix, where n is the number of unique words in the text. For each couple of words, we count the number of
 * times they co-occurred in the text in the same line.
 *
 * 4.2.1 Pairs Design Pattern
	 * The basic (and maybe most intuitive) implementation of this exercise is the Pair. The basic idea is to emit, for each
	 * couple of words in the same line, the couple itself (or pair) and the value 1. For example, in the line w1 w2 w3 w1,
	 * we emit (w1,w2):1, (w1,w3):1, (w2,w1):1, (w2,w3):1, (w2,w1):1, (w3,w1):1, (w3,w2):1, (w3,w1):1.
	 * In this exercise, we need to use a composite key to emit an occurrence of a pair of words. The student will understand
	 * how to create a custom Hadoop data type to be used as key type.
	 * A Pair is a tuple composed by two elements that can be used to ship two objects within a parent object. For this exercise
	 * the student has to implement a TextPair, that is a Pair that contains two words.
	 *
	 * Instructions
		 * There are two files related to this exercise:
		 * - TextPair.java: data structure to be implemented by the student. Besides the implementation of the data
 			*structure itself, the student has to implement the serialization Hadoop API (write and read Fields).
		 * - Pair.java: the implementation of a pair example using TextPair.java as datatype.
		 	* Once completed and compiled, create the jar and run it on the container using the text analyzed in the word count
		 	* exercise. As done before, save the output information, since they will be compared with the following exercise.
 */
public class Pair extends Configured implements Tool {
	public static class PairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		@Override
		public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String line = value.toString();
			Text firstWord = new Text();
			Text secondWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty())
					continue;
				for (String word2 : WORD_BOUNDARY.split(line)) {
					if (word2.isEmpty() || word.equals(word2)) //<-- Se le due parole sono le stesse: non produco la coppia
						continue;
					firstWord.set(word);
					secondWord.set(word2);
					context.write(new TextPair(firstWord, secondWord), new IntWritable(1));
				}
			}
		}
	}

	public static class PairReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("%s requires three arguments\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numreducer = Integer.parseInt(args[2]);

		Configuration conf = getConf();
		Job job = new Job(conf, "PairNewAPI");

		job.setJarByClass(Pair.class);

		job.setMapperClass(PairMapper.class);
		job.setReducerClass(PairReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numreducer);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Pair(), args);
		System.exit(res);
	}
}
