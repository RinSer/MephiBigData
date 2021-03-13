import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import bdtc.lab1.MetricWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, MetricWritable, LongWritable> mapDriver;
    private ReduceDriver<MetricWritable, LongWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, MetricWritable, LongWritable, Text, LongWritable> mapReduceDriver;

    private final String testMetric1 = "5, 1615575252, 25";
    private final MetricWritable metric1 = new MetricWritable();

    private final String testMetric2 = "5, 1615575253, 33";
    private final MetricWritable metric2 = new MetricWritable();

    private final String testMetric3 = "3, 1615575255, 55";
    private final MetricWritable metric3 = new MetricWritable();

    @Before
    public void setUp() throws IOException {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        metric1.parse(testMetric1);
        metric2.parse(testMetric2);
        metric3.parse(testMetric3);
    }

    @Test
    public void testMapper() throws IOException {
        LongWritable testScore = new LongWritable(metric1.getScore());
        mapDriver
                .withInput(new LongWritable(), new Text(testMetric1))
                .withOutput(metric1, testScore)
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<LongWritable> metrics = new ArrayList<>();
        metrics.add(new LongWritable(metric1.getScore()));
        metrics.add(new LongWritable(metric2.getScore()));
        reduceDriver
                .withInput(metric1, metrics)
                .withOutput(
                        metric1.getKey(),
                        new LongWritable(metric1.getScore() + metric2.getScore())
                )
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testMetric1))
                .withInput(new LongWritable(), new Text(testMetric2))
                .withInput(new LongWritable(), new Text(testMetric3))
                .withOutput(
                        metric1.getKey(),
                        new LongWritable(metric1.getScore() + metric2.getScore())
                )
                .withOutput(
                        metric3.getKey(),
                        new LongWritable(metric3.getScore())
                )
                .runTest();
    }
}
