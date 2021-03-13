package bdtc.lab1;

/**
 * Перечисление названий метрик с их идентификаторами
 */
public enum MetricsEnum {
    Node1CPU(1), Node1RAM(2), Node1ROM(3),
    Node2CPU(4), Node2RAM(5), Node2ROM(6),
    Node3CPU(7), Node3RAM(8), Node3ROM(9);

    public final int id;

    MetricsEnum(final int id) {
        this.id = id;
    }

    /**
     * Возвращает название метрики по её идентификатору
     * @param id
     * @return
     */
    public static String getNameById(int id) {
        for(MetricsEnum e : MetricsEnum.values()) {
            if(id == e.id)
                return e.name();
        }

        return null;
    }
}
