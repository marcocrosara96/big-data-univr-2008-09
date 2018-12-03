package ex2b;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

//import fr.eurecom.dsg.mapreduce.utils.StringToIntAssociativeArrayWritable;

/**
 *  ----------------- ESEMPIO --------------------
 * gino paolo lemon
 * uno due tre
 *
 * 		gino	paolo	lemon	uno	due	tre
 * gino		1		1		1	0	0	0
 * paolo   ....
 * lemon  ....
 * uno
 * due
 * tre
 *
 *
 * Pairs:
 *
 * (gino, paolo) -> 1
 * (gino, lemon) -> 1
 *
 * Stripes:
 *
 * gino -> [(paolo, 1), (lemon, 1)]
 */

/**
 * 4.2.2 Stripes Design Pattern
 * This approach is similar to the previous one: for each line, co-occurring pairs are generated. However, now, instead of
 * emitting every pair as soon as it is generated, intermediate results are stored in an associative array. We use an
 * associative array, and, for each word, we emit the word itself as key and a Stripe, that is the map of co-occurring words
 * with the number of associated occurrence.
 * For example, in the line w1 w2 w3 w1, we emit:
 * w1:{w2:1, w3:1}, w2:{w1:2,w3:1}, w3:{w1:2, w2:1}, w1:{w2:1, w3:1}
 * Note that, instead, we could emit also:
 * w1:{w2:2, w3:2}, w2:{w1:2,w3:1}, w3:{w1:2, w2:1}
 * In this exercise the st*/
public class Stripes extends Configured implements Tool {

	public static class StripesMapper extends Mapper<LongWritable, Text, Text, StringToIntAssociativeArrayWritable> {

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		@Override
		public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			StringToIntAssociativeArrayWritable dict = new StringToIntAssociativeArrayWritable();
			String line = value.toString();
			Text keyWord = new Text();

			for (String keyW : WORD_BOUNDARY.split(line)) {
				if (keyW.isEmpty())
					continue;
				for (String listW : WORD_BOUNDARY.split(line)) {
					if (listW.isEmpty() || keyW.equals(listW))
						continue;

					dict.put(listW, 1);
				}

				keyWord.set(keyW);
				context.write(keyWord, dict);
				dict.clear();
			}
		}
	}

	public static class StripesReducer extends Reducer<Text, StringToIntAssociativeArrayWritable, Text, StringToIntAssociativeArrayWritable> {

		@Override
		public void reduce(Text key, Iterable<StringToIntAssociativeArrayWritable> values, Context context) throws IOException, InterruptedException {
			StringToIntAssociativeArrayWritable aggreg = new StringToIntAssociativeArrayWritable();
			for (StringToIntAssociativeArrayWritable t: values) {
				aggreg.sum(t);
			}
			context.write(key, aggreg);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {

			System.err.printf("%s requires two arguments\n", getClass().getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numreducer = Integer.parseInt(args[2]);

		Configuration conf = getConf();
		Job job = new Job(conf, "PairNewAPI");

		job.setJarByClass(Stripes.class);

		job.setMapperClass(StripesMapper.class);
		job.setReducerClass(StripesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringToIntAssociativeArrayWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringToIntAssociativeArrayWritable.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numreducer);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Stripes(), args);
		System.exit(res);
	}
}
