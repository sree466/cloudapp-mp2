import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PopularityLeague(),
				args);
		System.exit(res);
	}

	public static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() {
			super(IntWritable.class);
		}

		public IntArrayWritable(Integer[] numbers) {
			super(IntWritable.class);
			IntWritable[] ints = new IntWritable[numbers.length];
			for (int i = 0; i < numbers.length; i++) {
				ints[i] = new IntWritable(numbers[i]);
			}
			set(ints);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/mp2/tmp");
		fs.delete(tmpPath, true);

		Job jobA = Job.getInstance(conf, "Link Count");
		jobA.setOutputKeyClass(IntWritable.class);
		jobA.setOutputValueClass(IntWritable.class);

		jobA.setMapOutputKeyClass(IntWritable.class);
		jobA.setMapOutputValueClass(IntWritable.class);

		jobA.setMapperClass(LinkCountMap.class);
		jobA.setReducerClass(LinkCountReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, tmpPath);

		jobA.setJarByClass(PopularityLeague.class);
		jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(conf, "Top Links");
		jobB.setOutputKeyClass(IntWritable.class);
		jobB.setOutputValueClass(IntWritable.class);

		jobB.setMapOutputKeyClass(NullWritable.class);
		jobB.setMapOutputValueClass(IntArrayWritable.class);

		jobB.setMapperClass(TopLinksMap.class);
		jobB.setReducerClass(TopLinksReduce.class);
		jobB.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);

		jobB.setJarByClass(PopularityLeague.class);
		return jobB.waitForCompletion(true) ? 0 : 1;

	}

	// TODO
	
	public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

	public static class LinkCountMap extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		// TODO
		
		List<Integer> leagues;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String leaguePath = conf.get("league");
	        this.leagues = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line, " :");
			boolean fromlink = true;
			while (st.hasMoreTokens()) {
				if (fromlink) {
					Integer key = Integer.parseInt(st.nextToken().trim()
							.toLowerCase());
					if(leagues.contains(linkid))
		        	{
						context.write(new IntWritable(key), new IntWritable(0));
		        	}
					fromlink = false;
				} else {
					Integer key = Integer.parseInt(st.nextToken().trim()
							.toLowerCase());
					if(leagues.contains(linkid))
		        	{
						context.write(new IntWritable(key), new IntWritable(1));
		        	}
				}

			}
		}
	}

	public static class LinkCountReduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		// TODO
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritabl(sum));
		}
	}

	public static class TopLinksMap extends
			Mapper<Text, Text, NullWritable, IntArrayWritable> {

		// TODO

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO
			Integer count = Integer.parseInt(value.toString());
			Integer linkid = Integer.parseInt(key.toString());
			if(leagues.contains(linkid))
        	{
				Integer[] strings = { linkid, count };
				IntArrayWritable val = new IntArrayWritable(strings);
				context.write(NullWritable.get(), val);        			
        	}			
		}
		
	}

	public static class TopLinksReduce extends
			Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
		
		private Map<Integer, Integer> countRankMap= new HashMap<Integer,Integer>();

		// TODO
		@Override
		public void reduce(NullWritable key, Iterable<IntArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO
			for (IntArrayWritable val : values) {
				Integer[] pair = (Integer[]) val.toArray();
				Integer count = pair[1];
				countRankMap.put(count, 0);			
			}
			
			for (Map.Entry<Integer, Integer> entry : countRankMap.entrySet()) {
				Integer key = entry.getKey();
	    	    Integer value = entry.getValue();
	    	    for (Map.Entry<Integer, Integer> inentry : countRankMap.entrySet()) {
					Integer inkey = entry.getKey();
		    	    Integer invalue = entry.getValue();		    	    
		    		if(key < inkey){
		    			countRankMap.put(inkey, invalue + 1);
		    		}		    		
		    	}
	    	}
			
			for (IntArrayWritable val : values) {
				Integer[] pair = (Integer[]) val.toArray();
				Integer linkid = pair[0];
				Integer count = pair[1];
				context.write(new IntWritable(linkid), new IntWritable(countRankMap.get(count)));		
			}		
			
		}
	}
}

class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
			A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}