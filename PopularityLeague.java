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
		
		List<Integer> leagues = new ArrayList<Integer>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String leaguePath = conf.get("league");
		        List<String> leaguesstr = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
			 for (String tempval : leaguesstr) {
				 Integer temint = Integer.parseInt(tempval);
                 this.leagues.add(temint);
             }

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
					Integer linkid = Integer.parseInt(st.nextToken().trim()
							.toLowerCase());
					if(this.leagues.contains(linkid))
		        	{
						context.write(new IntWritable(linkid), new IntWritable(0));
		        	}
					fromlink = false;
				} else {
					Integer linkid = Integer.parseInt(st.nextToken().trim()
							.toLowerCase());
					if(this.leagues.contains(linkid))
		        	{
						context.write(new IntWritable(linkid), new IntWritable(1));
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
			context.write(key, new IntWritable(sum));
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
				IntWritable[] pair = (IntWritable[]) val.toArray();
				Integer count = pair[1].get();
				this.countRankMap.put(count, 0);			
			}
			
			for (Map.Entry<Integer, Integer> entry : this.countRankMap.entrySet()) {
				Integer outkey = entry.getKey();
	    	    Integer value = entry.getValue();
	    	    for (Map.Entry<Integer, Integer> inentry : this.countRankMap.entrySet()) {
					Integer inkey = entry.getKey();
		    	    Integer invalue = entry.getValue();		    	    
		    		if(outkey < inkey){
		    			this.countRankMap.put(inkey, invalue + 1);
		    		}		    		
		    	}
	    	}
			
			for (IntArrayWritable val : values) {
				IntWritable[] pair = (IntWritable[]) val.toArray();
				Integer linkid = pair[0].get();
				Integer count = pair[1].get();
				context.write(new IntWritable(linkid), new IntWritable(this.countRankMap.get(count)));		
			}		
			
		}
	}
}