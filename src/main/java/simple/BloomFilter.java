/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package simple;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;

/**
 * BloomFilter 实现
 *
 * @param <E> BloomFilter中数据的类型 如，String or Integer.
 */
public class BloomFilter<E> implements Serializable {
    private BitSet bitset;
    private int bitSetSize;
    private double bitsPerElement;
    private int expectedNumberOfFilterElements; //可容纳的元素最大数
    private int numberOfAddedElements; //实际元素个数
    private int k; // 哈希函数个数

    static final Charset charset = Charset.forName("UTF-8");

    static final String hashName = "MD5";
    static final MessageDigest digestFunction;

    static {
        MessageDigest tmp;
        try {
            tmp = java.security.MessageDigest.getInstance(hashName);
        } catch (NoSuchAlgorithmException e) {
            tmp = null;
        }
        digestFunction = tmp;
    }

    /**
     * 创建Bloom filter,其长度： c*n.
     *
     * @param c 每个元素的bit 数.
     * @param n 最大容量
     * @param k 哈希函数个数
     */
    public BloomFilter(double c, int n, int k) {
        this.expectedNumberOfFilterElements = n;
        this.k = k;
        this.bitsPerElement = c;
        this.bitSetSize = (int) Math.ceil(c * n);
        numberOfAddedElements = 0;
        this.bitset = new BitSet(bitSetSize);
    }

    /**
     * 创建Bloom filter,自动推测哈希函数个数
     *
     * @param bitSetSize              总bit数大小
     * @param expectedNumberOElements 能容纳的元素数量
     */
    public BloomFilter(int bitSetSize, int expectedNumberOElements) {
        this(bitSetSize / (double) expectedNumberOElements, expectedNumberOElements, (int) Math.round((bitSetSize / (double) expectedNumberOElements) * Math
                .log(2.0)));
    }

    /**
     * 自动推测哈希函数个数
     *
     * @param falsePositiveProbability 错误率
     * @param expectedNumberOfElements 能容纳的元素数量
     */
    public BloomFilter(double falsePositiveProbability, int expectedNumberOfElements) {
        this(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
                expectedNumberOfElements, (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)))); // k = ceil(-log_2(false prob.))
    }

    /**
     * 基于已有BF创建新的 BF
     *
     * @param bitSetSize                     bitset的大小
     * @param expectedNumberOfFilterElements 能容纳的元素数
     * @param actualNumberOfFilterElements   实际写入的元素数
     * @param filterData                     已存在的BF对应的BitSet
     */
    public BloomFilter(int bitSetSize, int expectedNumberOfFilterElements, int actualNumberOfFilterElements, BitSet filterData) {
        this(bitSetSize, expectedNumberOfFilterElements);
        this.bitset = filterData;
        this.numberOfAddedElements = actualNumberOfFilterElements;
    }

    /**
     * 计算哈希值
     *
     * @param val     待计算哈希值的字符串
     * @param charset 字符集
     * @return 哈希值
     */
    public static int createHash(String val, Charset charset) {
        return createHash(val.getBytes(charset));
    }

    /**
     * 计算哈希值
     *
     * @param val 待计算哈希值的字符串,使用UTF8
     * @return 哈希值
     */
    public static int createHash(String val) {
        return createHash(val, charset);
    }

    /**
     * 计算哈希值
     *
     * @param data 待计算哈希值的字符串对应的bytes
     * @return 哈希值
     */
    public static int createHash(byte[] data) {
        return createHashes(data, 1)[0];
    }

    /**
     * 计算哈希值,将计算结果拆分到4-byte int型数组 .每次计算hash值时盐值加1.
     *
     * @param data   待计算哈希值的字符串对应的bytes
     * @param hashes 哈希函数个数
     * @return 多个哈希函数生成的哈希值
     */
    public static int[] createHashes(byte[] data, int hashes) {
        int[] result = new int[hashes];

        int k = 0;
        byte salt = 0;
        while (k < hashes) {
            byte[] digest;
            synchronized (digestFunction) {
                digestFunction.update(salt);
                salt++;
                digest = digestFunction.digest(data);
            }

            for (int i = 0; i < digest.length / 4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i * 4); j < (i * 4) + 4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    /**
     * 比较两个BF实例是否相等
     *
     * @param obj 比较的对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BloomFilter<E> other = (BloomFilter<E>) obj;
        if (this.expectedNumberOfFilterElements != other.expectedNumberOfFilterElements) {
            return false;
        }
        if (this.k != other.k) {
            return false;
        }
        if (this.bitSetSize != other.bitSetSize) {
            return false;
        }
        if (this.bitset != other.bitset && (this.bitset == null || !this.bitset.equals(other.bitset))) {
            return false;
        }
        return true;
    }

    /**
     * 计算BF的哈希值
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + (this.bitset != null ? this.bitset.hashCode() : 0);
        hash = 61 * hash + this.expectedNumberOfFilterElements;
        hash = 61 * hash + this.bitSetSize;
        hash = 61 * hash + this.k;
        return hash;
    }


    /**
     * 根据元素数量和BloomFilter的size自动计算错误率,如果BF中的元素比期望的少，实现的失误率会更低
     *
     * @return 错误率
     */
    public double expectedFalsePositiveProbability() {
        return getFalsePositiveProbability(expectedNumberOfFilterElements);
    }

    /**
     * 根据指定的元素数量和BloomFilter的size自动计算错误率
     *
     * @param numberOfElements 指定的元素数量
     * @return 错误率
     */
    public double getFalsePositiveProbability(double numberOfElements) {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow((1 - Math.exp(-k * (double) numberOfElements / (double) bitSetSize)), k);

    }

    /**
     * 根据元素数量和BloomFilter的size自动计算实际的错误率
     * @return 错误率.
     */
    public double getFalsePositiveProbability() {
        return getFalsePositiveProbability(numberOfAddedElements);
    }


    /**
     * 返回hash函数个数
     *
     * @return optimal k.
     */
    public int getK() {
        return k;
    }

    /**
     * 设置所有的bits为0
     */
    public void clear() {
        bitset.clear();
        numberOfAddedElements = 0;
    }

    /**
     * 增加元素
     *
     * @param element 待增加的元素
     */
    public void add(E element) {
        add(element.toString().getBytes(charset));
    }

    /**
     * 增加元素
     *
     * @param bytes 待增加的元素
     */
    public void add(byte[] bytes) {
        int[] hashes = createHashes(bytes, k);
        for (int hash : hashes)
            bitset.set(Math.abs(hash % bitSetSize), true);
        numberOfAddedElements++;
    }

    /**
     * 待增加的元素
     *
     * @param c 待增加的元素
     */
    public void addAll(Collection<? extends E> c) {
        for (E element : c)
            add(element);
    }

    /**
     * 是否包含元素
     *
     * @param element 元素
     * @return 是否包含元素
     */
    public boolean contains(E element) {
        return contains(element.toString().getBytes(charset));
    }

    /**
     * 是否包含元素
     *
     * @param bytes 元素
     * @return 是否包含元素
     */
    public boolean contains(byte[] bytes) {
        int[] hashes = createHashes(bytes, k);
        for (int hash : hashes) {
            if (!bitset.get(Math.abs(hash % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 是否包含元素
     *
     * @param c 元素
     * @return 是否全部包含c中的元素
     */
    public boolean containsAll(Collection<? extends E> c) {
        for (E element : c)
            if (!contains(element)) return false;
        return true;
    }

    /**
     * 读取bit
     *
     * @param bit bit的位置
     * @return true  bit的位置对应的值
     */
    public boolean getBit(int bit) {
        return bitset.get(bit);
    }

    /**
     * 设置bit
     *
     * @param bit   bit的位置
     * @param value bit的位置对应的值
     */
    public void setBit(int bit, boolean value) {
        bitset.set(bit, value);
    }

    /**
     * 返回全部bit
     *
     * @return bit 全部bit
     */
    public BitSet getBitSet() {
        return bitset;
    }

    /**
     * 返回bitset的大小
     *
     * @return bitset的大小
     */
    public int size() {
        return this.bitSetSize;
    }

    /**
     * 返回元素数
     *
     * @return 元素数
     */
    public int count() {
        return this.numberOfAddedElements;
    }

    /**
     * 返回期待的元素数，即能容纳的总元素数
     *
     * @return 能容纳的总元素数
     */
    public int getExpectedNumberOfElements() {
        return expectedNumberOfFilterElements;
    }

    /**
     * 返回每个元素对应的bit数量
     */
    public double getExpectedBitsPerElement() {
        return this.bitsPerElement;
    }

    /**
     * 返回每个元素实际对应的bit数量
     *
     * @return 每个元素实际对应的bit数量
     */
    public double getBitsPerElement() {
        return this.bitSetSize / (double) numberOfAddedElements;
    }
}