import java.io.IOException; 
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.io.*  ;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Music2 {
	public static void main (String[] args) throws Exception{
		Configuration c = new Configuration() ;
		String []files = new GenericOptionsParser(c,args).getRemainingArgs() ;
		Path input = new Path(files[0]) ;
		Path output = new Path(files[1]) ;
		Job j = new Job(c,"wordcount") ;
		j.setJarByClass(Music2.class);
		j.setMapperClass(MapforProg.class);
		j.setReducerClass(ReduceforProg.class);
		j.setOutputKeyClass(Text.class) ;
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j,input) ;
		FileOutputFormat.setOutputPath(j,output) ;
		System.exit(j.waitForCompletion(true)?0:1) ;
	}
	
	public static class MapforProg extends Mapper<LongWritable, Text,Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con)throws IOException,InterruptedException{
			String lines = value.toString() ;
			String [] line = lines.split("\n");
			
			for(String words : line){
				String[] data = words.split(",") ;
				Text outputkey = new Text(data[1]) ;
				IntWritable outputkeyval = new IntWritable(Integer.parseInt(data[2])) ;
				con.write(outputkey, outputkeyval);
			}
		}
	}
	public static class ReduceforProg extends Reducer<Text, IntWritable, Text, IntWritable>{
		Text maxword = new Text() ;
		int count = 0;
		public void reduce(Text word, Iterable<IntWritable>values, Context con)throws IOException,InterruptedException{
				int sum = 0 ;
				for(IntWritable value: values)
				{
					sum+=value.get();
				}
		con.write(word, new IntWritable(sum));				
		}
		
	}
}

