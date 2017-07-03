package simple;

import java.util.Collection;

/**
 * BloomFilter
 *
 * @author Michael.Wang
 * @date 2017/7/3
 */
public interface BloomFilter<E> {
    /**
     * 增加元素
     *
     * @param element 待增加的元素
     */
    public void add(E element);

    /**
     * 待增加的元素
     *
     * @param c 待增加的元素
     */
    public void addAll(Collection<? extends E> c);

    /**
     * 是否包含元素
     *
     * @param element 元素
     * @return 是否包含元素
     */
    public boolean contains(E element);

    /**
     * 返回bitset的大小
     *
     * @return bitset的大小
     */
    public int size();

    /**
     * 返回元素数
     *
     * @return 元素数
     */
    public int count();

}