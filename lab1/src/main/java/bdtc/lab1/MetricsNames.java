package bdtc.lab1;

/**
 * Названия метрик с их идентификаторами
 */
public class MetricsNames {

    private final static String[] metricNames = new String[] {
        "Node1CPU(1)", "Node1RAM(2)", "Node1ROM(3)",
        "Node2CPU(4)", "Node2RAM(5)", "Node2ROM(6)",
        "Node3CPU(7)", "Node3RAM(8)", "Node3ROM(9)"
    };

    /**
     * Возвращает название метрики по её идентификатору
     * @param id идентификатор в виде целого числа
     * @return название метрики в виде строки
     */
    public static String getNameById(int id) {
        if (id > 0 && id < metricNames.length) {
            return metricNames[id - 1];
        }
        return null;
    }
}
