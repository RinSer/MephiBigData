package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: находит среднее арифметическое всех показателей метрик,
 * полученных от {@link HW1Mapper},
 * выдаёт среднии метрик в срезе заданного диапазона времени
 */
public class HW1Reducer extends Reducer<MetricWritable, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(MetricWritable metricWithStamp, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long count = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            count++;
        }

        context.write(
            new Text(metricWithStamp.getFinalKey()),
            new LongWritable(sum / count)
        );
    }
}
