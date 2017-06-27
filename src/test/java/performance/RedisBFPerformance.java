package performance;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.test.MemoryBFTest;
import orestes.bloomfilter.test.helper.Helper;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

/**
 * Output:
 * False Positives = 5231, FP-Rate = 17.436666666666667
 * 107.078s, 280.1696 elements/s
 * 5.018s, 5978.4775 elements/s
 */
public class RedisBFPerformance {
    public static void main(String[] args) throws Exception {
        int items = 100_000_000;
        int count = 30_000;
        int m = 100_000;
        int k = 10;
        BloomFilter<String> b = Helper.createRedisFilter("ruby", m, k, HashMethod.Murmur2, true);
        compareToRuby(count, items, b);
        dumbParallelAdds(count, items, b);
        BloomFilter<String> b3 = Helper.createFilter(m * 10, k, HashMethod.Murmur2);
        dumbAdds(count * 10, items * 10, b);
    }

    private static void compareToRuby(int count, int items, BloomFilter<String> b) {
        b.clear();
        Random r = new Random();
        LongAdder longAdder = new LongAdder();
        Set<String> seen = Collections.newSetFromMap(new ConcurrentHashMap<>());
        long start = System.currentTimeMillis();
        IntStream.range(0, count).parallel().forEach(i -> {
            String element = String.valueOf(r.nextInt(items));
            if (b.contains(element) && !seen.contains(element)) {
                longAdder.increment();
            }
            b.add(element);
            seen.add(element);
        });
        double fprate = 100.0 * longAdder.intValue() / count;
        System.out.println("False Positives = " + longAdder + ", FP-Rate = " + fprate);
        long end = System.currentTimeMillis();
        MemoryBFTest.printStat(start, end, count);
    }

    private static void dumbParallelAdds(int count, int items, BloomFilter<String> b) throws Exception {
        b.clear();
        Random r = new Random();
        ExecutorService exec = Executors.newFixedThreadPool(20);
        long start = System.currentTimeMillis();
        IntStream.range(0, count).forEach(i -> {
            exec.submit(() -> b.add(String.valueOf(r.nextInt(items))));
        });
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        MemoryBFTest.printStat(start, end, count);
    }

    private static void dumbAdds(int count, int items, BloomFilter<String> b) throws Exception {
        b.clear();
        Random r = new Random();
        long start = System.currentTimeMillis();
        IntStream.range(0, count).forEach(i -> {
            b.add(String.valueOf(r.nextInt(items)));
        });
        long end = System.currentTimeMillis();
        MemoryBFTest.printStat(start, end, count);
    }

}
