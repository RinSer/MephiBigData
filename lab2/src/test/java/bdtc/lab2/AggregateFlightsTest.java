package bdtc.lab2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AggregateFlightsTest {

    /**
     * Проверяет правильность исполнения аггрегирующей функции
     * @throws IOException
     */
    @Test
    public void flightsShouldBeHourlyAggregated() throws IOException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("FlightsAggregationAppTest");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        List<Row> inputRows = new ArrayList();
        inputRows.add(RowFactory.create(convertToUtf16(
    "{\"number\": \"6945\", \"time\": \"2021-04-15T01:00:00+00:00\", \"departure\": \"Beijing Capital International\", \"arrival\": \"Wuxi\"}")));
        inputRows.add(RowFactory.create(convertToUtf16(
    "{\"number\": \"6913\", \"time\": \"2021-04-15T01:00:00+00:00\", \"departure\": \"Beijing Capital International\", \"arrival\": \"Hangzhou\"}")));
        inputRows.add(RowFactory.create(convertToUtf16(
    "{\"number\": \"1083\", \"time\": \"2021-04-15T00:45:00+00:00\", \"departure\": \"Beijing Capital International\", \"arrival\": \"Nanjing Lukou International Airport\"}")));
        inputRows.add(RowFactory.create(convertToUtf16(
    "{\"number\": \"6865\", \"time\": \"2021-04-15T00:40:00+00:00\", \"departure\": \"Beijing Capital International\", \"arrival\": \"Hangzhou\"}")));
        inputRows.add(RowFactory.create(convertToUtf16(
    "{\"number\": \"6909\", \"time\": \"2021-04-15T00:35:00+00:00\", \"departure\": \"Beijing Capital International\", \"arrival\": \"Shenzhen\"}")));

        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType);

        Dataset<Row> testStream = spark.createDataFrame(inputRows, schema);

        String airportsFilePath = "../app/data/airports2countries.txt";
        Dataset<Row> output = FlightsAggregationApp.aggregateFlights(testStream, airportsFilePath);

        List<String> flights = output.select(
                output.col("time"),
                output.col("departure"),
                output.col("arrival"),
                output.col("count"))
                .map((MapFunction<Row, String>) r ->
                        r.getTimestamp(0) + "," + r.getString(1) + r.getString(2) + "," + r.getLong(3),
                        Encoders.STRING())
                .collectAsList();

        Assert.assertEquals(2, flights.size());
        Assert.assertTrue(flights.contains("2021-04-15 00:00:00.0,ChinaChina,3"));
        Assert.assertTrue(flights.contains("2021-04-15 01:00:00.0,ChinaChina,2"));
    }

    private byte[] convertToUtf16(String initial)
    {
        return initial.getBytes(StandardCharsets.UTF_16);
    }
}
