package ex3;

import java.io.IOException;
import java.util.regex.Pattern;

import ex2.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import fr.eurecom.dsg.mapreduce.utils.TextPair;

public class OrderInversion extends Configured implements Tool {

	private final static String ASTERISK = "\0";
	private final static Text ASTERISKTEXT = new Text(ASTERISK);
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {

		/**
		 * Implement getPartition such that pairs with the same first element
		 * will go to the same reducer.
		 */
		@Override
		public int getPartition(TextPair key, IntWritable value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
			//la "& Integer.MAX_VALUE" viene fatto per essere sicuro il valore ritornato sia positivo
		}
	}

	public static class PairMapper extends
	Mapper<LongWritable, Text, TextPair, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String line = value.toString();
			Text firstWord = new Text();
			Text secondWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty())
					continue;
				for (String word2 : WORD_BOUNDARY.split(line)) {
					if (word2.isEmpty() || word.equals(word2))
						continue;
					firstWord.set(word);
					secondWord.set(word2);
					context.write(new TextPair(firstWord, secondWord), new IntWritable(1));
					context.write(new TextPair(firstWord, ASTERISKTEXT), new IntWritable(1)); // <---- ATTENZIONE!! Importante
				}
			}
		}
	}

	public static class PairReducer extends
	Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

		private int cooccurrence_count;
		private int s;

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			if(key.getSecond().equals(ASTERISKTEXT)){
				cooccurrence_count = 0;
				for (IntWritable count : values) {
					cooccurrence_count += count.get();
				}
			}
			else {
				s = 0;
				for (IntWritable count : values) {
					s += count.get();
				}
				context.write(key, new DoubleWritable((double)s / cooccurrence_count));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {

			System.err.printf("%s requires two arguments\n", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numreducer = Integer.parseInt(args[2]);

		Configuration conf = getConf();
		Job job = new Job(conf, "Pair Relative");

		job.setJarByClass(OrderInversion.class);

		job.setMapperClass(PairMapper.class);
		job.setReducerClass(PairReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(PartitionerTextPair.class);
		// job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(TextPair.Comparator.class);

		job.setNumReduceTasks(numreducer);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OrderInversion(), args);
		System.exit(res);
	}
}
