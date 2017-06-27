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
     *
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
     * 构建对象 ，自动计算缺失的参数 (如： bit size).
     *
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


    public boolean redisBacked() {
        return redisBacked;
    }


    public int expectedElements() {
        return expectedElements;
    }

    /**
     * @return Bloom filter的大小，以bit为单位
     */
    public int size() {
        return size;
    }

    /**
     * @return Bloom filter使用的哈希函数数量
     */
    public int hashes() {
        return hashes;
    }

    /**
     * @return 用于计数的bits位数
     */
    public int countingBits() {
        return countingBits;
    }

    /**
     * @return 可容忍的假阳率
     */
    public double falsePositiveProbability() {
        return falsePositiveProbability;
    }

    /**
     * @return Bloom filter的名称
     */
    public String name() {
        return name;
    }

    /**
     * @return redis host
     */
    public String redisHost() {
        return redisHost;
    }

    /**
     * @return redis port
     */
    public int redisPort() {
        return redisPort;
    }

    /**
     * @return redis连接的数量
     */
    public int redisConnections() {
        return redisConnections;
    }

    /**
     * @return redis 连接是否使用SSL
     */
    public boolean redisSsl() {
        return redisSsl;
    }

    /**
     * @return Bloom filter待使用的hash函数
     */
    public HashMethod hashMethod() {
        return hashMethod;
    }

    /**
     * @return Bloom filter实际使用的hash函数
     */
    public HashFunction hashFunction() {
        return hashFunction;
    }

    /**
     * @return 字符串转换为bytes时使用的字符集
     */
    public static Charset defaultCharset() {
        return defaultCharset;
    }

    /**
     * @return {@code true} 如果同名的bloomFilter已经存在，是否覆盖
     */
    public boolean overwriteIfExists() {
        return overwriteIfExists;
    }

    /**
     * @return 返回所有的redis slave
     */
    public Set<Entry<String, Integer>> getReadSlaves() {
        return slaves;
    }

    /**
     * 检查两个bf的大小和哈希函数确认是否兼容
     *
     * @param other 另一个哈希函数的配置
     * @return {@code true} 配置是否兼容
     */
    public boolean isCompatibleTo(FilterBuilder other) {
        return this.size() == other.size() && this.hashes() == other.hashes() && this.hashMethod() == other.hashMethod();
    }

    /**
     * 根据给定的元素数量<i>expectedElements</i>和假阳率<i>falsePositiveProbability</i>，自动计算bf的bits大小
     *
     * @param n 期待的插入BloomFilter的元素数量
     * @param p 可容忍的假阳率
     * @return 计算出的BloomFilter的bits大小
     */
    public static int optimalM(long n, double p) {
        return (int) Math.ceil(-1 * (n * Math.log(p)) / Math.pow(Math.log(2), 2));
    }

    /**
     * 根据给定的元素数量<i>expectedElements</i>和BloomFilter的大小(bits)，自动计算hash函数的个数
     *
     * @param n 期待的插入BloomFilter的元素数量
     * @param m BloomFilter的bits大小
     * @return 计算出的hash函数的个数
     */
    public static int optimalK(long n, long m) {
        return (int) Math.ceil((Math.log(2) * m) / n);
    }

    /**
     * 根据配置的大小和函数数量计算元素个数 [TD]
     *
     * @param k 哈希函数个数
     * @param m BloomFilter的大小
     * @return 元素个数(amount of elements a Bloom filter for which the given configuration of size and hashes is optimal.)
     */
    public static int optimalN(long k, long m) {
        return (int) Math.ceil((Math.log(2) * m) / k);
    }

    /**
     * 计算最好情况下的假阳率(Calculates the best-case (uniform hash function) false positive probability.) [TD]
     *
     * @param k                哈希函数个数
     * @param m                BloomFilter的bits大小
     * @param insertedElements 插入BloomFilter的元素个数
     * @return 计算出的假阳率
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
