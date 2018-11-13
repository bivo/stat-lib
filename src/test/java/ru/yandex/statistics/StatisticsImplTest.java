package ru.yandex.statistics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class StatisticsImplTest {

    private Statistics statistics;

    @Before
    public void init() {
        statistics = new StatisticsImpl();
    }

    @Test
    public void basicTest() {
        addEvent();
        checkStatistics(1, 1, 1);

        addEvent();
        checkStatistics(2, 2, 2);
    }

    @Test
    public void testBucketsRolling() throws InterruptedException {
        addEvent();
        checkStatistics(1, 1, 1);

        TimeUnit.SECONDS.sleep(65);
        checkStatistics(0, 1, 1);

        addEvent();
        checkStatistics(1, 2, 2);
    }

    @Test
    public void eventOneMinuteAgo() {
        addEvent(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));
        checkStatistics(0, 1, 1);
    }

    @Test
    public void eventFiftyNineMinutesAgo() {
        addEvent(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(59));
        checkStatistics(0, 1, 1);
    }

    @Test
    public void eventTwentyHoursAgo() {
        addEvent(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(20));
        checkStatistics(0, 0, 1);
    }

    @Test
    public void eventFromThePast() {
        addEvent(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2));
        checkStatistics(0, 0, 0);
    }

    @Test(expected = IllegalStateException.class)
    public void eventFromTheFuture() {
        addEvent(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));
    }

    @Test
    public void stressTest() {
        int quantity = 100_000;

        long startTime = System.currentTimeMillis();
        addEvents(quantity);
        long endTime = System.currentTimeMillis();

        checkStatistics(quantity, quantity, quantity);
        System.out.println(quantity + " events processed in " + (endTime - startTime) + " ms");
    }

    /**
     * Тест генеририрует большое кол-во старых событий, что вызвает большое кол-во итераций в StatisticsImpl
     * для поиска нужного тайм-слота. Тест показывает что в этом краевом кейсе производительность снижается,
     * но находится на приемлемом уровне.
     */
    @Test
    public void stressTestWithOldEvents() {
        int quantity = 100_000;

        long startTime = System.currentTimeMillis();
        addEvents(quantity, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(22));
        long endTime = System.currentTimeMillis();

        checkStatistics(0, 0, quantity);
        System.out.println(quantity + " events processed in " + (endTime - startTime) + " ms");
    }

    @Test
    public void addEventsFromMultipleThreads() throws InterruptedException {
        int quantity = 100_000;

        ExecutorService executor = Executors.newFixedThreadPool(32);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < quantity; i++) {
            executor.submit((Runnable) this::addEvent);
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        checkStatistics(quantity, quantity, quantity);
        System.out.println(quantity + " events processed in " + (endTime - startTime) + " ms");
    }

    // Utility methods

    private void addEvent() {
        addEvent(System.currentTimeMillis());
    }

    private void addEvent(Long timestamp) {
        statistics.addEvent(timestamp);
    }

    private void addEvents(int quantity) {
        addEvents(quantity, System.currentTimeMillis());
    }

    private void addEvents(int quantity, Long timestamp) {
        IntStream
                .range(0, quantity)
                .forEach(i -> addEvent(timestamp));
    }

    private void checkStatistics(int expectedEventsLastMinute, int expectedEventsLastHour, int expectedEventsLastDay) {
        Assert.assertEquals(expectedEventsLastMinute, statistics.countEventsLastMinute());
        Assert.assertEquals(expectedEventsLastHour, statistics.countEventsLastHour());
        Assert.assertEquals(expectedEventsLastDay, statistics.countEventsLastDay());
    }

}
