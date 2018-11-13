package ru.yandex.statistics;

import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StatisticsImpl implements Statistics {

    private static final int MINUTES_IN_DAY = (int) TimeUnit.DAYS.toMinutes(1);

    /**
     * Deque, который разделяет сутки на 1440 (60 мин * 24 ч) слота.
     * Каждый слот хранит счётчик с кол-ом событий в эту минуту.
     */
    private final Deque<AtomicLong> eventBuckets;

    public StatisticsImpl() {
        eventBuckets = initBuckets();
        startRollingTask();
    }

    @Override
    public void addEvent(long timestamp) {
        long millisAgo = System.currentTimeMillis() - timestamp;
        if (millisAgo < 0) {
            throw new IllegalStateException("The timestamp is from the future or time on the server is wrong");
        }
        long minAgo = TimeUnit.MILLISECONDS.toMinutes(millisAgo);

        if (minAgo >= MINUTES_IN_DAY) {
            // событие произошло больше 24 часов назад, не будем его учитывать
            return;
        }

        Iterator<AtomicLong> iterator = eventBuckets.iterator();

        AtomicLong eventBucket = iterator.next();
        for (int i = 0; i < minAgo; i++) {
            eventBucket = iterator.next();
        }
        eventBucket.incrementAndGet();
    }

    @Override
    public long countEventsLastMinute() {
        return countEvents(1);
    }

    @Override
    public long countEventsLastHour() {
        return countEvents(60);
    }

    @Override
    public long countEventsLastDay() {
        return countEvents(MINUTES_IN_DAY);
    }

    private long countEvents(int forLastMinutes) {
        Iterator<AtomicLong> iterator = eventBuckets.iterator();

        long counter = 0;
        for (int i = 0; i < forLastMinutes; i++) {
            AtomicLong bucketCounter = iterator.next();
            counter += bucketCounter.get();
        }

        return counter;
    }

    private Deque<AtomicLong> initBuckets() {
        ConcurrentLinkedDeque<AtomicLong> eventBuckets = new ConcurrentLinkedDeque<>();

        for (int i = 0; i < MINUTES_IN_DAY; i++) {
            eventBuckets.add(new AtomicLong());
        }

        return eventBuckets;
    }

    /**
     * Периодический таск, который запускается раз в минуту и:
     *   - удаляет один старый слот
     *   - добавляет один новый слот
     * За счёт этого поддерживается хранение статистики событий за последние сутки.
     */
    private void startRollingTask() {
        TimerTask rollingTask = new TimerTask() {
            @Override
            public void run() {
                eventBuckets.addFirst(new AtomicLong());
                eventBuckets.pollLast();
            }
        };

        // синхронизируемся, чтобы первый роллинг произошёл в начале следующей минуты
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
        long millisSinceMinuteStart =
                TimeUnit.SECONDS.toMillis(now.get(Calendar.SECOND)) + now.get(Calendar.MILLISECOND);
        long millisTillNextMinute = TimeUnit.MINUTES.toMillis(1) - millisSinceMinuteStart;

        // а дальше будем делать роллинг раз в минуту
        long period = TimeUnit.MINUTES.toMillis(1);

        new Timer().schedule(rollingTask, millisTillNextMinute, period);
    }

}
