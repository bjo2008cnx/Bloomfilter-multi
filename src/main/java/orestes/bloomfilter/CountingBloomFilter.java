package orestes.bloomfilter;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 可计数的bf，允许删除元素
 */
public interface CountingBloomFilter<T> extends BloomFilter<T> {

    /**
     * @return 用于计数的bit位数
     */
    public default int getCountingBits() {
        return config().countingBits();
    }


    @Override
    public default boolean addRaw(byte[] element) {
        return addAndEstimateCountRaw(element) == 1;
    }

    /**
     * 删除元素
     *
     * @param element 待删除元素
     * @return {@code true} 删除元素后元素对应的bits是否都为0
     */
    public default boolean removeRaw(byte[] element) {
        return removeAndEstimateCountRaw(element) <= 0;
    }

    /**
     * 删除元素
     *
     * @param element 待删除元素
     * @return {@code true}  删除元素后元素对应的bits是否都为0
     */
    public default boolean remove(T element) {
        return removeRaw(toBytes(element));
    }


    /**
     * 删除元素
     *
     * @param elements 待删除元素
     * @return 删除元素后元素对应的bits是否都为0的结果列表
     */
    public default List<Boolean> removeAll(Collection<T> elements) {
        return elements.stream().map(this::remove).collect(Collectors.toList());
    }

    /**
     * 返回使用最小选择算法情况下元素的估算计数(如：.选择最小计数器).
     * 这个估计有点偏向, 没有考虑bf是否已满，但在实践中表现非常好
     * 其理论基础是 spectral Bloom filters, 参考: http://theory.stanford.edu/~matias/papers/sbf_thesis.pdf
     *
     * @param element 待查询的元素
     * @return 估计的计数
     */
    public long getEstimatedCount(T element);

    /**
     * 添加一个元素并返回元素被添加的次数
     * @param element 待添加的元素
     * @return 元素被添加的次数
     */
    public long addAndEstimateCountRaw(byte[] element);

    /**
     * 添加一个元素并返回元素被添加的次数
     *
     * @param element 待添加的元素
     * @return 元素被添加的次数
     */
    public default long addAndEstimateCount(T element) {
        return addAndEstimateCountRaw(toBytes(element));
    }

    /**
     * 删除一个元素并返回元素被添加的次数
     *
     * @param element 待删除的元素
     * @return 元素被添加的次数
     */
    public long removeAndEstimateCountRaw(byte[] element);

    /**
     * 删除一个元素并返回元素被添加的次数
     *
     * @param element 待删除的元素
     * @return 元素被添加的次数
     */
    public default long removeAndEstimateCount(T element) {
        return removeAndEstimateCountRaw(toBytes(element));
    }

    /**
     * @return clone
     */
    public CountingBloomFilter<T> clone();

}
