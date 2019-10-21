import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatrixMul {
    
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private Text location = new Text();
        private Text recordValue = new Text();

        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
			Map<Text,Integer> count=new HashMap<Text, Integer>();

			String[] element=value.toString().split(",");
			
			element[0]=element[0].replace("[","").replaceAll("\"","");
			element[1]=element[1].trim();
			element[2]=element[2].trim();
			element[3]=element[3].replace("]","").trim();
			/*
			System.out.println(element[0]);
			System.out.println(element[1]);
			System.out.println(element[2]);
			System.out.println(element[3]);
			System.out.println("----------");
			*/
			int row=Integer.parseInt(element[1]);
			int col=Integer.parseInt(element[2]);
			int elementValue=Integer.parseInt(element[3]);
			if(element[0].equals("a")){
					for(int j=0;j<5;j++){
							location.set(row+","+j);
							recordValue.set("a,"+element[2]+","+element[3]);
							context.write(location,recordValue);
					}
			}
			else{
					for(int i=0;i<5;i++){
							location.set(i+","+col);
							recordValue.set("b,"+element[1]+","+element[3]);
							context.write(location,recordValue);
					}
			}
        }
    }
    
    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            Map<Integer,Integer> matrixA=new HashMap<Integer,Integer>();
			Map<Integer,Integer> matrixB=new HashMap<Integer,Integer>();
			Text result=new Text();
			for(Text v:values){
				System.out.println(v.toString());
				String[] element=v.toString().split(",");
				if(element[0].equals("a")){
					matrixA.put(Integer.parseInt(element[1]),Integer.parseInt(element[2]));		
				}
				else if(element[0].equals("b")){
					matrixB.put(Integer.parseInt(element[1]),Integer.parseInt(element[2]));
				}
				else{
					System.out.print("A nor B:");
					System.out.println(v.toString());
					continue;		
				}
			}
			int sum=0;
			for(int j=0;j<5;j++){
				int a=matrixA.containsKey(j)? matrixA.get(j):0;
				int b=matrixB.containsKey(j)? matrixB.get(j):0;
				sum+=a*b;
			}
			result.set(Integer.toString(sum));
            context.write(key,result);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix multiplication");
        
        job.setJarByClass(MatrixMul.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
    
}

