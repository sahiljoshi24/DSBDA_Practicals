import java.io.IOException; 
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.io.*  ;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Movie {
	public static void main (String[] args) throws Exception{
		Configuration c = new Configuration() ;
		String []files = new GenericOptionsParser(c,args).getRemainingArgs() ;
		Path input = new Path(files[0]) ;
		Path output = new Path(files[1]) ;
		Job j = new Job(c,"wordcount") ;
		j.setJarByClass(Movie.class);
		j.setMapperClass(MapforProg.class);
		j.setReducerClass(ReduceforProg.class);
		j.setOutputKeyClass(Text.class) ;
		j.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(j,input) ;
		FileOutputFormat.setOutputPath(j,output) ;
		System.exit(j.waitForCompletion(true)?0:1) ;
	}
	
	public static class MapforProg extends Mapper<LongWritable, Text,Text, DoubleWritable>{
		public void map(LongWritable key, Text value, Context con)throws IOException,InterruptedException{
			String lines = value.toString() ;
			String [] line = lines.split("\n");
			
			for(String words : line){
				String[] data = words.split(",") ;
				Text outputkey = new Text(data[1]) ;
				DoubleWritable outputkeyval = new DoubleWritable(Math.round(Float.parseFloat(data[2]))) ;
				con.write(outputkey, outputkeyval);
			}
		}
	}
	public static class ReduceforProg extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		Text maxword = new Text() ;
		double max = 0 ;
		int count = 0;
		public void reduce(Text word, Iterable<DoubleWritable>values, Context con)throws IOException,InterruptedException{
				int sum = 0 ;
				for(DoubleWritable value: values)
				{
					sum+=value.get();
					count++;
				}
				double avg = sum/(count*1.0000);
				if(avg>max){
					max = avg ;
					maxword.set(word);
				}				
		}
		
		protected void cleanup(Context con)throws IOException, InterruptedException,InterruptedException{			
			con.write(new Text("Suggested movie with highest rating:" + maxword), new DoubleWritable(max));
		}
	}
}