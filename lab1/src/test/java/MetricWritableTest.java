import bdtc.lab1.MetricWritable;
import bdtc.lab1.MetricsNames;
import org.apache.commons.lang.math.IntRange;
import org.junit.Test;

import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Юнит тесты функционала, энкапсулируемого
 * кастомным классом метрик
 */
public class MetricWritableTest {

    private final Map<Character, Integer> spanMap = new HashMap<Character, Integer>() {{
        put('s', 1); put('m', 60); put('h', 3600);
        put('d', (24 * 3600)); put('w', (7 * 24 * 3600));
    }};

    private final Character[] possibleSpans = spanMap.keySet().toArray(new Character[5]);

    /**
     * Проверям корректность инициализации кастомного класса метрик:
     * должен принимать в качестве параметра временной диапазон в
     * определенном формате и сопоставлять его с делителями секунд
     * или же выбрасывать исключение.
     */
    @Test
    public void testCreation() {
        int[] sizes = new IntRange(0, 1000).toArray();
        for (char c : possibleSpans) {
            for (int i : sizes) {
                MetricWritable metric = new MetricWritable(String.valueOf(i) + c);
                assertEquals("Scale units should be correct", c, metric.scaleUnit);
                assertEquals("Scale size should be correct", i, metric.scaleSize);
                assertEquals("Scale span should be correct", i * spanMap.get(c), metric.span);
            }
        }
        String[] impossibleSpans = new String[] { "ss", "mm", "a", "b", "cde" };
        for (String s : impossibleSpans) {
            for (int i : sizes) {
                try {
                    MetricWritable metric = new MetricWritable(i + s);
                } catch (InputMismatchException ex) {
                    assertEquals(
                            "Should throw exception on invalid scale input",
                            "Scale should match regex [\\d+(s|m|h|d|w)]!",
                            ex.getMessage()
                    );
                }
            }
        }
    }

    /**
     * Проверяем корректность парсинга строк из входного файла:
     * должны правильно считываться идентификатор метрики,
     * отметка времени и значение, в случае несоответствия
     * строки шаблону должно выводиться исключение.
     */
    @Test
    public void testParsing() {
        String[] possibleInputLines = new String[] {
                "1, 1615575309, 30558", "2, 1615580000, 3", "5, 1615600000, 55555"
        };
        int[] sizes = new IntRange(1, 1000).toArray();
        for (char c : possibleSpans) {
            for (int i : sizes) {
                MetricWritable metric = new MetricWritable(String.valueOf(i) + c);
                for (String line : possibleInputLines) {
                    metric.parse(line);
                    String[] lineParts = line.split(metric.separator + "\\s*");
                    int metricScore = Integer.parseInt(lineParts[2]);
                    assertEquals("Metric value should parse correctly", metricScore, metric.getScore());
                    int timestamp = Integer.parseInt(lineParts[1]) / (i * spanMap.get(c));
                    int metricId = Integer.parseInt(lineParts[0]);
                    assertEquals(
                "Metric id and timestamp should parse correctly",
                MetricsNames.getNameById(metricId) + metric.separator + timestamp + metric.separator + i + c,
                        metric.getFinalKey().toString()
                    );
                }
            }
        }
        String[] impossibleInputLines = new String[] {
                "", "2 1615580000, 3", "5, gfh, 55555", "aaa", "123 fs df ae"
        };
        for (char c : possibleSpans) {
            for (int i : sizes) {
                MetricWritable metric = new MetricWritable(String.valueOf(i) + c);
                for (String line : impossibleInputLines) {
                    try {
                        metric.parse(line);
                    } catch (InputMismatchException ex) {
                        assertEquals(
                                "Should throw exception on invalid input line",
                                "Each input line should match regex [\\d+, \\d+, \\d+]!",
                                ex.getMessage()
                        );
                    }
                }
            }
        }
    }
}
