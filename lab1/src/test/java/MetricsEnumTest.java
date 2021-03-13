import bdtc.lab1.MetricsEnum;
import org.apache.commons.lang.math.IntRange;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Тесты перечисления метрик
 */
public class MetricsEnumTest {

    String[] metricNames;

    /**
     * Заполняем проверочный массив со всеми возможными
     * названиями метрик, сохранив их порядок
     * @throws IOException
     */
    @Before
    public void setup() throws IOException {
        metricNames = new String[9];
        for(MetricsEnum e : MetricsEnum.values()) {
            metricNames[e.ordinal()] = e.name();
        }
    }

    /**
     * Проверяем, что перечисление с названиями метрик
     * правильно отдаёт название по числовому идентификатору
     * @throws IOException
     */
    @Test
    public void testDevicesEnumConversionToString() throws IOException  {
        int[] metricIds = new IntRange(1, 9).toArray();
        for (int i : metricIds) {
            String currentName = MetricsEnum.getNameById(i);
            assertEquals(metricNames[i-1], currentName);
        }
    }
}