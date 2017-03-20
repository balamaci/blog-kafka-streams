package com.balamaci;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.concurrent.TimeUnit;

/**
 * @author sbalamaci
 */
public class TimerTest {

    private static final Logger log = LoggerFactory.getLogger(TimerTest.class);

    @Test
    public void timerOperator() {
        Observable.timer(5, TimeUnit.SECONDS)
                .toBlocking()
                .subscribe(
                        tick -> log.info("Tick {}", tick),
                        (ex) -> log.info("Error emitted"),
                        () -> log.info("Completed"));
    }

    @Test
    public void overlappingWindow() {
        Observable<Integer> numbers = Observable.range(1, 10);
        Observable<Long> timer = Observable.interval(2, TimeUnit.SECONDS);

        Observable<Integer> periodicNumbersEmitter = Observable.zip(numbers, timer, (key, val) -> key);

        Observable<Integer> delayedNumbersWindow = periodicNumbersEmitter
                .window(10, 5, TimeUnit.SECONDS)
                .flatMap(window -> window.doOnCompleted(() -> log.info("Window completed")));

        delayedNumbersWindow
                .toBlocking()
                .subscribe(number -> log.info("Got {}", number));
    }


}
