import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import bdtc.lab1.MetricWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Тесты функционала маппера, редьюсера и мапредьюсера
 */
public class MapReduceTest {

    private MapDriver<LongWritable, Text, MetricWritable, LongWritable> mapDriver;
    private ReduceDriver<MetricWritable, LongWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, MetricWritable, LongWritable, Text, LongWritable> mapReduceDriver;

    private final String testMetric1 = "5, 1615575252, 25";
    private final MetricWritable metric1 = new MetricWritable("1m");

    private final String testMetric2 = "5, 1615575253, 33";
    private final MetricWritable metric2 = new MetricWritable("1m");

    private final String testMetric3 = "3, 1615575255, 55";
    private final MetricWritable metric3 = new MetricWritable("1m");

    private final String testMetric4 = "5, 1615575253, 77";
    private final MetricWritable metric4 = new MetricWritable("1m");

    /**
     * Подготавливаем и конфигурируем драйверы для маппера, редьюсера
     * и мапредьюсера, а также тестовые метрики.
     * @throws IOException
     */
    @Before
    public void setUp() throws IOException {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.getContext().getConfiguration().set("scale", "1m");
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.getConfiguration().set("scale", "1m");
        metric1.parse(testMetric1);
        metric2.parse(testMetric2);
        metric3.parse(testMetric3);
        metric4.parse(testMetric4);
    }

    /**
     * Проверяем работу маппера: при передачи строки
     * он должен вернуть объект метрики и её значение.
     * @throws IOException
     */
    @Test
    public void testMapper() throws IOException {
        LongWritable testScore = new LongWritable(metric1.getScore());
        mapDriver
                .withInput(new LongWritable(), new Text(testMetric1))
                .withOutput(metric1, testScore)
                .runTest();
    }

    /**
     * Проверяем работу редьюсера: должен возвращать верные
     * среднии значения для одной, двух и трех аггрегируемых метрик.
     * @throws IOException
     */
    @Test
    public void testReducer() throws IOException {
        List<LongWritable> metrics = new ArrayList<>();
        metrics.add(new LongWritable(metric1.getScore()));
        reduceDriver
                .withInput(metric1, metrics)
                .withOutput(metric1.getKey(), new LongWritable(metric1.getScore()));
        metrics.add(new LongWritable(metric2.getScore()));
        long average = (metric1.getScore() + metric2.getScore()) / 2;
        reduceDriver
                .withInput(metric2, metrics)
                .withOutput(metric2.getKey(), new LongWritable(average));
        metrics.add(new LongWritable(metric4.getScore()));
        average = (metric1.getScore() + metric2.getScore() + metric4.getScore()) / 3;
        reduceDriver
                .withInput(metric4, metrics)
                .withOutput(metric4.getKey(), new LongWritable(average))
                .runTest();
    }

    /**
     * Проверяем работу мапредьюсера: получив на вход две метрики
     * или одну он должен их правильно саггрегировать.
     * @throws IOException
     */
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testMetric1))
                .withInput(new LongWritable(), new Text(testMetric2))
                .withInput(new LongWritable(), new Text(testMetric3))
                .withOutput(
                        metric1.getKey(),
                        new LongWritable((metric1.getScore() + metric2.getScore())/2)
                )
                .withOutput(
                        metric3.getKey(),
                        new LongWritable(metric3.getScore())
                )
                .runTest();
    }
}
