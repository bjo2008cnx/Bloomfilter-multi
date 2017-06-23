package orestes.bloomfilter;

import orestes.bloomfilter.HashProvider.HashFunction;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.*;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

/**
 * bf 构造器
 */
public class FilterBuilder implements Cloneable, Serializable {
    private boolean redisBacked = false;
    private boolean overwriteIfExists = false;
    private Integer expectedElements;
    private Integer size;
    private Integer hashes;
    private Integer countingBits = 16;
    private Double falsePositiveProbability;
    private String name = "";
    private String redisHost = "localhost";
    private Integer redisPort = 6379;
    private Integer redisConnections = 10;
    private boolean redisSsl = false;
    private HashMethod hashMethod = HashMethod.Murmur3KirschMitzenmacher;
    private HashFunction hashFunction = HashMethod.Murmur3KirschMitzenmacher.getHashFunction();
    private Set<Entry<String, Integer>> slaves = new HashSet<>();
    private static transient Charset defaultCharset = Charset.forName("UTF-8");
    private boolean done = false;
    private String password = null;
    private RedisPool pool;
    private int database = Protocol.DEFAULT_DATABASE;

    public FilterBuilder() {
    }

    /**
     * 指定bf的元素个数和可容忍的错误率,根据bf的大小自动设置hash函数个数
     *
     * @param expectedElements         bf大小
     * @param falsePositiveProbability 可容忍的错误率
     */
    public FilterBuilder(int expectedElements, double falsePositiveProbability) {
        this.expectedElements(expectedElements).falsePositiveProbability(falsePositiveProbability);
    }

    /**
     * 指定bf大小和hash函数个数
     *
     * @param size   bit size of the Bloom filter
     * @param hashes number of hash functions to use
     */
    public FilterBuilder(int size, int hashes) {
        this.size(size).hashes(hashes);
    }

    /**
     * 设定元素个数. 根据容忍的失败率自动计算大小和hash函数个数
     *
     * @param expectedElements 元素个数
     * @return 修改后的对象
     */
    public FilterBuilder expectedElements(int expectedElements) {
        this.expectedElements = expectedElements;
        return this;
    }

    /**
     * 设置bits的大小
     *
     * @param size bits大小
     * @return 修改后的对象
     */
    public FilterBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * 设置可容忍的失败率
     *
     * @param falsePositiveProbability the tolerable false
     * @return 修改后的对象
     */
    public FilterBuilder falsePositiveProbability(double falsePositiveProbability) {
        this.falsePositiveProbability = falsePositiveProbability;
        return this;
    }

    /**
     * 设置hash函数个数
     *
     * @param numberOfHashes hash函数个数
     * @return 修改后的对象
     */
    public FilterBuilder hashes(int numberOfHashes) {
        this.hashes = numberOfHashes;
        return this;
    }

    /**
     * 设置用于计数的bits个数
     *
     * @param countingBits 用于计数的bits个数
     * @return 修改后的对象
     */
    public FilterBuilder countingBits(int countingBits) {
        this.countingBits = countingBits;
        return this;
    }

    /**
     * 设置bf名称 ，如果在redis中已存在，并且配置相同，已存在的bf将被使用并返回,可使用{@link #overwriteIfExists(boolean)}进行修改
     *
     * @param name The name of the filter
     * @return 修改后的对象
     */
    public FilterBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     *
     *
     * @param password The Redis PW
     * @return 修改后的对象
     */
    public FilterBuilder password(String password) {
        this.password = password;
        return this;
    }

    /**
     * 设置连接池
     *
     * @param pool The RedisPool
     * @return 修改后的对象
     */
    public FilterBuilder pool(RedisPool pool) {
        this.redisBacked(true);
        this.pool = pool;
        return this;
    }

    /**
     * 设置是否使用redis作为存储,默认false
     *
     * @param redisBacked 是否使用redis作为存储
     * @return 修改后的对象
     */
    public FilterBuilder redisBacked(boolean redisBacked) {
        this.redisBacked = redisBacked;
        return this;
    }

    /**
     * 设置host
     * @param host the Redis host
     * @return 修改后的对象
     */
    public FilterBuilder redisHost(String host) {
        this.redisBacked = true;
        this.redisHost = host;
        return this;
    }

    /**
     * 缺省为 6379
     *
     * @param port the Redis port
     * @return 修改后的对象
     */
    public FilterBuilder redisPort(int port) {
        this.redisBacked = true;
        this.redisPort = port;
        return this;
    }


    /**
     * 设置redis的最大连接数，默认: 10 [TD]
     *
     * @param numConnections redis的最大连接数
     * @return 修改后的对象
     */
    public FilterBuilder redisConnections(int numConnections) {
        this.redisBacked = true;
        this.redisConnections = numConnections;
        return this;
    }

    /**
     * 是否使用ssl连接到redis ，缺省为false
     *
     * @param ssl 是否使用ssl
     * @return 修改后的对象
     */
    public FilterBuilder redisSsl(boolean ssl) {
        this.redisBacked = true;
        this.redisSsl = ssl;
        return this;
    }

    /**
     * 如果在redis中相同name的已存在，是否覆盖
     *
     * @param overwrite 是否覆盖
     * @return 修改后的对象
     */
    public FilterBuilder overwriteIfExists(boolean overwrite) {
        this.overwriteIfExists = overwrite;
        return this;
    }

    /**
     * 添加redis slave以加快读的速度 (如：contains 或 getEstimatedCount 操作)  Slave节点必须在redis中是实例的从库 (可通过redis-cli 的 SLAVEOF 命令)设置。
     * 因redis的同步是异步的，有可能从从库读到过期数据
     *
     * @param host host of Redis slave
     * @param port port of Redis slave
     * @return 修改后的对象
     */
    public FilterBuilder addReadSlave(String host, int port) {
        slaves.add(new SimpleEntry<>(host, port));
        return this;
    }


    /**
     * 设置hash算法，应使用HashProvider中的{@link HashProvider.HashMethod}枚举值.。缺省为MD5
     *
     * @param hashMethod the method used to generate hash values
     * @return 修改后的对象
     */
    public FilterBuilder hashFunction(HashMethod hashMethod) {
        this.hashMethod = hashMethod;
        this.hashFunction = hashMethod.getHashFunction();
        return this;
    }

    /**
     * 指定hash函数
     *
     * @param hf the custom hash function
     * @return 修改后的对象
     */
    public FilterBuilder hashFunction(HashFunction hf) {
        this.hashFunction = hf;
        return this;
    }

    /**
     * 指定数据库数量[TD]
     *
     * @param database number
     * @return 修改后的对象
     */
    public FilterBuilder database(int database) {
        this.database = database;
        return this;
    }

    public int database() {
        return database;
    }

    /**
     * 构建对象 ，自动计算缺失的参数 (如： bit size).
     *
     * @param <T> 元素类型.
     * @return Bloomfilter实例
     */
    public <T> BloomFilter<T> buildBloomFilter() {
        complete();
        if (redisBacked) {
            return new BloomFilterRedis<>(this);
        } else {
            return new BloomFilterMemory<>(this);
        }
    }

    /**
     *
     * 构建对象 ，自动计算缺失的参数 (如： bit size).
     * @param <T> the type of element contained in the Counting Bloom filter.
     * @return the constructed Counting Bloom filter
     */
    public <T> CountingBloomFilter<T> buildCountingBloomFilter() {
        complete();
        if (redisBacked) {
            return new CountingBloomFilterRedis<>(this);
        } else {
            if (countingBits == 32) {
                return new CountingBloomFilter32<>(this);
            } else if (countingBits == 16) {
                return new CountingBloomFilter16<>(this);
            } else if (countingBits == 8) {
                return new CountingBloomFilter8<>(this);
            } else if (countingBits == 64) {
                return new CountingBloomFilter64<>(this);
            } else {
                return new CountingBloomFilterMemory<>(this);
            }
        }
    }

    /**
     * 检查参数并自动设置缺失参数
     *
     * @return the completed FilterBuilder
     */
    public FilterBuilder complete() {
        if (done) {
            return this;
        }
        if (size == null && expectedElements != null && falsePositiveProbability != null) {
            size = optimalM(expectedElements, falsePositiveProbability);
        }
        if (hashes == null && expectedElements != null && size != null) {
            hashes = optimalK(expectedElements, size);
        }
        if (size == null || hashes == null) {
            throw new NullPointerException("Neither (expectedElements, falsePositiveProbability) nor (size, hashes) were specified.");
        }
        if (expectedElements == null) {
            expectedElements = optimalN(hashes, size);
        }
        if (falsePositiveProbability == null) {
            falsePositiveProbability = optimalP(hashes, size, expectedElements);
        }

        done = true;
        return this;
    }


    @Override
    public FilterBuilder clone() {
        Object clone;
        try {
            clone = super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Cloning failed.");
        }
        return (FilterBuilder) clone;
    }


    /**
     * @return {@code true} if the Bloom Filter will be Redis-backed
     */
    public boolean redisBacked() {
        return redisBacked;
    }

    /**
     * @return the number of expected elements for the Bloom filter
     */
    public int expectedElements() {
        return expectedElements;
    }

    /**
     * @return the size of the Bloom filter in bits
     */
    public int size() {
        return size;
    }

    /**
     * @return the number of hashes used by the Bloom filter
     */
    public int hashes() {
        return hashes;
    }

    /**
     * @return The number of bits used for counting in case of a counting Bloom filter
     */
    public int countingBits() {
        return countingBits;
    }

    /**
     * @return the tolerable false positive probability of the Bloom filter
     */
    public double falsePositiveProbability() {
        return falsePositiveProbability;
    }

    /**
     * @return the name of the Bloom filter
     */
    public String name() {
        return name;
    }

    /**
     * @return the host name of the Redis server backing the Bloom filter
     */
    public String redisHost() {
        return redisHost;
    }

    /**
     * @return the port used by the Redis server backing the Bloom filter
     */
    public int redisPort() {
        return redisPort;
    }

    /**
     * @return the number of connections used by the Redis Server backing the Bloom filter
     */
    public int redisConnections() {
        return redisConnections;
    }

    /**
     * @return if SSL is enabled for Redis connection
     */
    public boolean redisSsl() {
        return redisSsl;
    }

    /**
     * @return The hash method to be used by the Bloom filter
     */
    public HashMethod hashMethod() {
        return hashMethod;
    }

    /**
     * @return the actual hash function to be used by the Bloom filter
     */
    public HashFunction hashFunction() {
        return hashFunction;
    }

    /**
     * @return Return the default Charset used for conversion of String values into byte arrays used for hashing
     */
    public static Charset defaultCharset() {
        return defaultCharset;
    }

    /**
     * @return {@code true} if the Bloom filter that is to be built should overwrite any existing Bloom filter with the
     * same name
     */
    public boolean overwriteIfExists() {
        return overwriteIfExists;
    }

    /**
     * @return return the list of all read slaves to be used by the Redis-backed Bloom filter
     */
    public Set<Entry<String, Integer>> getReadSlaves() {
        return slaves;
    }

    /**
     * Checks whether a configuration is compatible to another configuration based on the size of the Bloom filter and
     * its hash functions.
     *
     * @param other the other configuration
     * @return {@code true} if the configurations are compatible
     */
    public boolean isCompatibleTo(FilterBuilder other) {
        return this.size() == other.size() && this.hashes() == other.hashes() && this.hashMethod() == other.hashMethod();
    }

    /**
     * Calculates the optimal size <i>size</i> of the bloom filter in bits given <i>expectedElements</i> (expected
     * number of elements in bloom filter) and <i>falsePositiveProbability</i> (tolerable false positive rate).
     *
     * @param n Expected number of elements inserted in the bloom filter
     * @param p Tolerable false positive rate
     * @return the optimal size <i>size</i> of the bloom filter in bits
     */
    public static int optimalM(long n, double p) {
        return (int) Math.ceil(-1 * (n * Math.log(p)) / Math.pow(Math.log(2), 2));
    }

    /**
     * Calculates the optimal <i>hashes</i> (number of hash function) given <i>expectedElements</i> (expected number of
     * elements in bloom filter) and <i>size</i> (size of bloom filter in bits).
     *
     * @param n Expected number of elements inserted in the bloom filter
     * @param m The size of the bloom filter in bits.
     * @return the optimal amount of hash functions hashes
     */
    public static int optimalK(long n, long m) {
        return (int) Math.ceil((Math.log(2) * m) / n);
    }

    /**
     * Calculates the amount of elements a Bloom filter for which the given configuration of size and hashes is
     * optimal.
     *
     * @param k number of hashes
     * @param m The size of the bloom filter in bits.
     * @return amount of elements a Bloom filter for which the given configuration of size and hashes is optimal.
     */
    public static int optimalN(long k, long m) {
        return (int) Math.ceil((Math.log(2) * m) / k);
    }

    /**
     * Calculates the best-case (uniform hash function) false positive probability.
     *
     * @param k                number of hashes
     * @param m                The size of the bloom filter in bits.
     * @param insertedElements number of elements inserted in the filter
     * @return The calculated false positive probability
     */
    public static double optimalP(long k, long m, double insertedElements) {
        return Math.pow((1 - Math.exp(-k * insertedElements / (double) m)), k);
    }


    public String password() {
        return password;
    }

    public RedisPool pool() {
        if (done && pool == null) {
            pool = RedisPool.builder().host(redisHost()).port(redisPort()).readSlaves(getReadSlaves()).password(password()).database(database())
                    .redisConnections(redisConnections()).build();
        }
        return pool;
    }
}
