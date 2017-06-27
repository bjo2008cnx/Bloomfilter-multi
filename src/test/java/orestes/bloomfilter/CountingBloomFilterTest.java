package orestes.bloomfilter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by lenovo on 2017/6/23.
 */
public class CountingBloomFilterTest {

    public static final String HOST = "10.12.64.189";
    public static final int PORT = 6301;

    @Test
    public void getCountingBits() throws Exception {

    }

    @Test
    public void addRaw() throws Exception {

    }

    @Test
    public void removeRaw() throws Exception {

    }

    @Test
    public void remove() throws Exception {

    }

    @Test
    public void removeAll() throws Exception {

    }

    private CountingBloomFilter<String> createBF() {
        final int objects = 500;
        int m = 1001;
        int k = 10;
        String name = "CountingTests";
        return new FilterBuilder(m, k).name(name).redisBacked(true).redisHost(HOST).redisPort(PORT).buildCountingBloomFilter();
    }

    @Test
    public void getEstimatedCount() throws Exception {
        CountingBloomFilter<String> b = createBF();
        System.out.println("Size of bloom filter: " + b.getSize() + ", hash functions: " + b.getHashes());
        b.clear();
        b.add("Käsebrot");
        b.add("ist");
        b.add("ein");
        b.add("gutes");
        b.add("Brot");
        b.add("ist");
        b.add("ist");
        long count = b.getEstimatedCount("ist");
        assertEquals(3, count);
        b.clear();
    }

    @Test
    public void addAndEstimateCountRaw() throws Exception {

    }

    @Test
    public void addAndEstimateCount() throws Exception {
        CountingBloomFilter<String> b = createBF();
        System.out.println("Size of bloom filter: " + b.getSize() + ", hash functions: " + b.getHashes());
        //TODO 为啥这里的size是100
        b.clear();
        b.add("ist");
        b.add("Käsebrot");
        b.add("ein");
        b.add("Brot");
        b.add("ist");
        b.add("ist");
        long count = b.addAndEstimateCount("ist");
        assertEquals(4, count);
        b.clear();
    }

    @Test
    public void removeAndEstimateCountRaw() throws Exception {

    }

    @Test
    public void removeAndEstimateCount() throws Exception {

    }

    @Test
    public void clonex() throws Exception {

    }

}