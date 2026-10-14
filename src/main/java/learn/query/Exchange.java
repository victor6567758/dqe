package learn.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Exchange: the boundary where data moves between fragments.
 * In a real cluster this is a network shuffle. Here it waits on local futures.
 */
public final class Exchange {

    private Exchange() {}

    public static <T> List<T> gather(List<Future<T>> futures) throws Exception {
        List<T> results = new ArrayList<>();
        for (Future<T> future : futures) {
            results.add(future.get());
        }
        return results;
    }
}
