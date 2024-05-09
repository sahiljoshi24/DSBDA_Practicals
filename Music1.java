import java.io.IOException; 
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.io.*  ;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Music1 
{
	public static void main (String[] args) throws Exception
	{
		Configuration c = new Configuration() ;
		String []files = new GenericOptionsParser(c,args).getRemainingArgs() ;
		Path input = new Path(files[0]) ;
		Path output = new Path(files[1]) ;
		Job j = new Job(c,"wordcount") ;
		j.setJarByClass(Music1.class);
		j.setMapperClass(MapforProg.class);
		j.setReducerClass(ReduceforProg.class);
		j.setOutputKeyClass(Text.class) ;
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j,input) ;
		FileOutputFormat.setOutputPath(j,output) ;
		System.exit(j.waitForCompletion(true)?0:1) ;
	}
	
	public static class MapforProg extends Mapper<LongWritable, Text,Text, Text>
	{
		public void map(LongWritable key, Text value, Context con)throws IOException,InterruptedException
			{
			    String lines = value.toString() ;
					String[] data = lines.split(",") ;
					Text userid = new Text(data[0]) ;
					Text songid = new Text(data[1]) ;
					con.write(songid, userid);
			}
	}
	public static class ReduceforProg extends Reducer<Text, Text, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<Text>songs, Context con)throws IOException,InterruptedException
		{
				HashSet<String> users = new HashSet<>();
				for(Text song: songs)
				{
					users.add(song.toString());
				}
		con.write(word, new IntWritable(users.size()));				
		}
		
	}
}
