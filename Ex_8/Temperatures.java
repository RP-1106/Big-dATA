import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperatures {

    public static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            
            if (fields.length >= 8) {  // Ensure there are enough fields
                try {
                    // Extract the minimum and maximum temperature fields
                    int minTemp = Integer.parseInt(fields[6].trim());  // MLY-TMIN-NORMAL
                    int maxTemp = Integer.parseInt(fields[7].trim());  // MLY-TMAX-NORMAL

                    // Write the min and max temperatures to the context
                    context.write(new Text("Min Temperature"), new IntWritable(minTemp));
                    context.write(new Text("Max Temperature"), new IntWritable(maxTemp));
                } catch (NumberFormatException e) {
                    // Ignore invalid data
                }
            }
        }
    }

    public static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int extremeTemp = key.toString().equals("Min Temperature") ? Integer.MAX_VALUE : Integer.MIN_VALUE;

            for (IntWritable value : values) {
                int temp = value.get();
                if (key.toString().equals("Min Temperature")) {
                    if (temp < extremeTemp) {
                        extremeTemp = temp;
                    }
                } else {  // Max Temperature
                    if (temp > extremeTemp) {
                        extremeTemp = temp;
                    }
                }
            }
            context.write(key, new IntWritable(extremeTemp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "min and max temperatures");
        job.setJarByClass(Temperatures.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
