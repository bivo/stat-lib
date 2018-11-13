package ru.yandex.statistics;

public interface Statistics {

    /**
     * Добавляет событие. В случае если событие произошло
     * больше, чем 24 часа назад, событие не учитывается.
     *
     * @param timestamp временная метка (в UTC)
     */
    void addEvent(long timestamp);

    /**
     * @return кол-во событий за последнюю минуту
     */
    long countEventsLastMinute();

    /**
     * @return кол-во событий за последний час (60 минут)
     */
    long countEventsLastHour();

    /**
     * @return кол-во событий за последние сутки (24 часа)
     */
    long countEventsLastDay();

}
