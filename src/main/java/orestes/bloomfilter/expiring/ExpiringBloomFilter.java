package orestes.bloomfilter.expiring;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * 可过期的计数BF
 *
 */
public interface ExpiringBloomFilter<T> extends CountingBloomFilter<T> {

    /**
     * 判断是否被缓存(存在且未过期)
     *
     * @param element 元素(或元素的 id)
     * @return <code>true</code> 是否被缓存
     */
    boolean isCached(T element);

    /**
     * 获取元素的过期时间戳
     *
     * @param element 元素(或元素的 id)
     * @param unit    时间单位
     * @return 过期时间戳
     */
    Long getRemainingTTL(T element, TimeUnit unit);

    /**
     * 获取元素的过期时间戳
     *
     * @param elements 元素
     * @param unit    时间单位
     * @return 过期时间戳
     */
    default List<Long> getRemainingTTLs(List<T> elements, TimeUnit unit){
        return elements.stream().map(el -> getRemainingTTL(el, unit)).collect(Collectors.toList());
    }

    /**
     * Reports a read on element that is to be cached for a certain ttl[TD]
     *
     * @param element 元素(或元素的 id)
     * @param TTL     时间单位
     * @param unit    the time unit of the provided ttl
     */
    void reportRead(T element, long TTL, TimeUnit unit);

    /**
     * Reports a write on an object, adding it to the underlying Bloom filter for the remaining ttl
     *
     * @param element 元素(或元素的 id)
     * @param unit 时间单位
     * @return 剩余过期时间。如果已过期，返回<code>null</code>
     */
    Long reportWrite(T element, TimeUnit unit);

    /**
     * Reports a write.
     *
     * @param element 元素(或元素的 id)
     * @return <code>true</code>, if the elements needs invalidation
     */
    default boolean reportWrite(T element) {
        return reportWrite(element, TimeUnit.MILLISECONDS) != null;
    }

    BloomFilter<T> getClonedBloomFilter();
}
