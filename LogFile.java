
	import java.io.IOException;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.util.GenericOptionsParser;

public class LogFile {
	public static void main(String [] args) throws Exception
	{
	Configuration c=new Configuration();
	String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
	Path input=new Path(files[0]);
	Path output=new Path(files[1]);
	Job j=new Job(c,"LogFile");
	j.setJarByClass(LogFile.class);
	j.setMapperClass(MapForLogFile.class);
	j.setReducerClass(ReduceForLogFile.class);
	j.setOutputKeyClass(Text.class);
	j.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(j, input);
	FileOutputFormat.setOutputPath(j, output);
	System.exit(j.waitForCompletion(true)?0:1);
	}
	public static class MapForLogFile extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	{
	String data = value.toString();
	String[] lines=data.split("\n");
	for(String line: lines )
	{     
	  String[] fields = line.split(",");
	  Text outputKey = new Text(fields[1]);
	  String time = fields[5].split(" ")[1];
	int secs = Integer.parseInt(time.split(":")[2]);
	secs += Integer.parseInt(time.split(":")[1])*60;
	secs += Integer.parseInt(time.split(":")[0])*3600;
	  IntWritable outputValue = new IntWritable(secs);
	  con.write(outputKey, outputValue);
	}
	}
	}

	public static class ReduceForLogFile extends Reducer<Text, IntWritable, Text, IntWritable>
	{        
	int max = 0;     
	Text maxip = new Text();        
	int min=99999;           
	Text minip = new Text();
	public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
	{
	int sum = 0;
	   for(IntWritable value : values)
	   {
	   sum += value.get();
	   }
	if(sum>max)
	{
		max=sum;
		maxip.set(word);
	}
	if(sum<min)
	{
		min=sum;
		minip.set(word);
	}
	}
	protected void cleanup (Context con) throws IOException, InterruptedException
	{
		con.write(new Text(maxip), new IntWritable(max));
	   	con.write(new Text(minip), new IntWritable(min));
	}
	}
}

