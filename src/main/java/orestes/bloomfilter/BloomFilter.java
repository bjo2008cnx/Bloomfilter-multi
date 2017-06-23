package orestes.bloomfilter;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * BloomFiler 接口
 */
public interface BloomFilter<T> extends Cloneable, Serializable {

    /**
     * 添加到bf
     *
     * @param element 待添加的元素
     * @return {@code true} 在bf中对应的bits是否已存在.
     */
    boolean addRaw(byte[] element);

    /**
     * 添加到bf
     *
     * @param element 待添加的元素
     * @return {@code true} 在bf中对应的bits是否已存在.
     */
    public default boolean add(T element) {
        return addRaw(toBytes(element));
    }

    /**
     * 批量添加
     *
     * @param elements 待添加的元素
     * @return 在bf中对应的bits是否已存在.
     */
    public default List<Boolean> addAll(Collection<T> elements) {
        return elements.stream().map(this::add).collect(Collectors.toList());
    }

    /**
     * 删除全部元素
     */
    public void clear();

    /**
     * 是否已存在
     *
     * @param element 待比较的元素
     * @return {@code true} 是否已存在
     */
    public boolean contains(byte[] element);

    /**
     * 是否已存在
     *
     * @param element 待比较的元素
     * @return {@code true} 是否已存在
     */
    public default boolean contains(T element) {
        return contains(toBytes(element));
    }

    /**
     * 是否已存在
     *
     * @param elements 待比较的元素
     * @return 是否已存在
     */
    public default List<Boolean> contains(Collection<T> elements) {
        return elements.stream().map(this::contains).collect(Collectors.toList());
    }

    /**
     * B是否已存在
     *
     * @param elements 待比较的元素
     * @return {@code true} 是否已存在
     */
    public default boolean containsAll(Collection<T> elements) {
        return elements.stream().allMatch(this::contains);
    }

    /**
     * 获取bf 对应的bitset
     *
     * @return 对应的bitset
     */
    public BitSet getBitSet();

    /**
     * 获取配置
     *
     * @return 配置
     */
    public FilterBuilder config();

    /**
     * 深拷贝
     *
     * @return 拷贝的bf
     */
    public BloomFilter<T> clone();

    /**
     * 获取大小, i.e. 即bit vector 的 大小
     *
     * @return bit vector 的 大小
     */
    public default int getSize() {
        return config().size();
    }

    /**
     * 获取期望的元素?
     *
     * @return 期望的元素
     */
    public default int getExpectedElements() {
        return config().expectedElements();
    }

    /**
     * 获取hash函数个数
     *
     * @return hash函数个数
     */
    public default int getHashes() {
        return config().hashes();
    }

    /**
     * 获取失效率，跟已放入的元素个数无关
     *
     * @return 失效率
     */
    public default double getFalsePositiveProbability() {
        return config().falsePositiveProbability();
    }

    /**
     * 转换为字节
     *
     * @param element 待转换的元素
     * @return 转换的字节
     */
    public default byte[] toBytes(T element) {
        return element.toString().getBytes(FilterBuilder.defaultCharset());
    }

    /**
     * 检查两个bf是否兼容, 如.参数是否兼容 (哈希函数, 大小等.)
     *
     * @param bloomFilter the bloomfilter
     * @param other       the other bloomfilter
     * @return <code>true</code> 是否兼容
     * @see #compatible(BloomFilter)
     */
    @Deprecated
    public default boolean compatible(BloomFilter<T> bloomFilter, BloomFilter<T> other) {
        return bloomFilter.compatible(other);
    }

    /**
     * 检查两个bf是否兼容, 如.参数是否兼容 (哈希函数, 大小等.)
     *
     * @param other the other bloomfilter
     * @return <code>true</code>是否兼容
     */
    public default boolean compatible(BloomFilter<T> other) {
        return config().isCompatibleTo(other.config());
    }

    /**
     * 清除bf的元素与元数据
     */
    public default void remove() {
        clear();
    }

    /**
     * 返回hash值
     *
     * @param bytes input element
     * @return hash values
     */
    public default int[] hash(byte[] bytes) {
        return config().hashFunction().hash(bytes, config().size(), config().hashes());
    }

    /**
     * 计算字符串的hash值
     *
     * @param value 待hash的字符串
     * @return array with <i>hashes</i> 哈希位置，范围为<i>[0,size)</i>
     */
    public default int[] hash(String value) {
        return hash(value.getBytes(FilterBuilder.defaultCharset()));
    }

    /**
     * 合并两个bf
     *
     * @param other the other bloom filter
     * @return <tt>true</tt> if this bloom filter could successfully be updated through the union with the provided
     * bloom filter
     */
    boolean union(BloomFilter<T> other);

    /**
     * 取交集
     *
     * @param other the other bloom filter
     * @return <tt>true</tt> if this bloom filter could successfully be updated through the intersection with the
     * provided bloom filter
     */
    boolean intersect(BloomFilter<T> other);

    /**
     * Returns {@code true} 是否为空
     *
     * @return {@code true}  是否为空
     */
    boolean isEmpty();

    /**
     * 获取错误率(大约): <br> <code>(1 - e^(-hashes * insertedElements /size)) ^ hashes</code>
     *
     * @param insertedElements 已写入bf的元素个数
     * @return probability of a false positive after <i>expectedElements</i> {@link #addRaw(byte[])} operations
     */
    public default double getFalsePositiveProbability(double insertedElements) {
        return FilterBuilder.optimalP(config().hashes(), config().size(), insertedElements);
    }

    /**
     * 根据目前已存在的元素计算可能的错误率(大约)
     *
     * @return probability of a false positive
     */
    public default double getEstimatedFalsePositiveProbability() {
        return getFalsePositiveProbability(getEstimatedPopulation());
    }


    /**
     * 基于已存在元素的expected number<i>expectedElements</i>.计算每个元素的bit数量
     *
     * @param n 已存在的元素个数
     * @return 每个元素的bit数
     */
    public default double getBitsPerElement(int n) {
        return config().size() / (double) n;
    }

    /**
     * 判断bit为0的可能性
     *
     * @param n 已存在的元素个数
     * @return 判断在 <i>expectedElements</i> {@link #addRaw(byte[])} 操作之后 bit为0的可能性
     */
    public default double getBitZeroProbability(int n) {
        return Math.pow(1 - (double) 1 / config().size(), config().hashes() * n);
    }

    /**
     * 估算bf的元素数量(see: http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter)
     *
     * @return 估算的bf的元素数量
     */
    public default Double getEstimatedPopulation() {
        return population(getBitSet(), config());
    }

    public static Double population(BitSet bitSet, FilterBuilder config) {
        int oneBits = bitSet.cardinality();
        return -config.size() / ((double) config.hashes()) * Math.log(1 - oneBits / ((double) config.size()));
    }

    /**
     * 打印元数据和bits
     *
     * @return bf的字符串表示
     */
    public default String asString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Bloom Filter Parameters: ");
        sb.append("size = " + config().size() + ", ");
        sb.append("hashes = " + config().hashes() + ", ");
        sb.append("Bits: " + getBitSet().toString());
        return sb.toString();
    }


}
