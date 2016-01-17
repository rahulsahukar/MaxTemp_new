import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException 
    {
        long total = 0;
        long count = 0;
        int maxValue = Integer.MAX_VALUE;
        for (IntWritable val : values)
        {
            maxValue = Math.min(maxValue, val.get());
            //total += val.get();
            total = maxValue;
            //count++;
        }
        //maxValue = (int)(total/count);
        context.write(key, new IntWritable(maxValue));
    }
}
