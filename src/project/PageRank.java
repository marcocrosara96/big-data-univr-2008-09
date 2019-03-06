package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

public class PageRank extends Configured implements Tool {

	static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();	//output key
		private Text outValue = new Text();	//output value
		private String pagerankType = "P";	//identifier of the pagerank value
		private String neighListType = "N";	//identifier of the node structure

		@Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lista = line.split("\t");
			String nextNodes = ""; // "\t a1 \t a2 \t a3"
			int n_figli = lista.length-2;
			double rank_figli = Double.parseDouble(lista[1]) / n_figli;
			for (int i=2; i< lista.length; i++) {
				outKey = new Text(lista[i]);
				outValue = new Text(pagerankType + rank_figli);
				context.write(outKey, outValue);

				nextNodes += "\t" + lista[i];
			}

			outKey = new Text(lista[0]);
			outValue = new Text(neighListType + nextNodes.substring(1));
			context.write(outKey, outValue);
		}
	}

	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();
		//private String pagerankType = "P";	//identifier of the pagerank value
		private char nodeType = 'N';	//identifier of the node structure

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			String nextNodes = "";
			for (Text text : values) {
				String v = text.toString();
				if(v.charAt(0) == nodeType)
					nextNodes = v.substring(1);
				else{
					sum += Double.parseDouble(v.substring(1));
				}
			}
			context.write(key, new Text(sum + "\t" + nextNodes));
		}
	}


	public static void deleteDir(Path dir) throws IOException {
		Boolean isRecusrive = true;

	    Configuration conf = new Configuration();
	    conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    FileSystem  hdfs = FileSystem.get(URI.create("hdfs://quickstart.cloudera/localhost"), conf);
	    hdfs.delete(dir, isRecusrive);
    }


	@Override
	public int run(String[] argsRun) throws Exception {
		if (argsRun.length != 2) {

			System.err.printf("%s requires three arguments (input folder, output folder, iterations)\n", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("PageRank_" + conf.get("recursion.depth"));

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path in,out;
		in = (new Path(argsRun[0]));
		out = (new Path(argsRun[1]));

		FileInputFormat.addInputPath(job, in);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, out);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}




	public static void main(String[] args) throws Exception {
		Configuration conf;

		int depth = 1; // variable to keep track of the recursion depth
		Path in,out;
		in = new Path(args[0]);
		out = new Path(args[1]);
		String[] newargs = {in.toString(), out.toString() + "_1"};
		int ITERATIONS = Integer.parseInt(args[2]);

		int res = -1;

		while (depth <= ITERATIONS) {
			conf = new Configuration();
			conf.set("recursion.depth", depth + "");

			res = ToolRunner.run(new Configuration(), new PageRank(), newargs);
			if(depth != 1 || depth != ITERATIONS)
				deleteDir(new Path(out.toString() + "_" + (depth - 1)));

			newargs[0] = out.toString() + "_" + depth;
			if(depth == ITERATIONS - 1)
				newargs[1] = out.toString();
			else
				newargs[1] = out.toString() + "_" + (depth+1);
			depth++;
		}

		System.exit(res);
	}
}

