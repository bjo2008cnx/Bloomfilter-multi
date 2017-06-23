package orestes.bloomfilter.redis;

import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.util.SafeEncoder;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * Redis提供的bitset. 并非所有方法都实现.如果有需要，可用 {@link #asBitSet()}.转换成传统的BitSet<br>
 */
public class RedisBitSet extends BitSet {
    private final RedisPool pool;
    private String name;
    private int size;

    /**
     * 构建bitset
     *
     * @param pool the redis pool
     * @param name redis中key的名称
     * @param size RedisBitSet的初始化大小
     */
    public RedisBitSet(RedisPool pool, String name, int size) {
        this.pool = pool;
        this.name = name;
        this.size = size;
    }


    @Override
    public boolean get(int bitIndex) {
        return pool.allowingSlaves().safelyReturn(jedis -> jedis.getbit(name, bitIndex));
    }

    /**
     * 获取指定位置的值.
     *
     * @param indexes 指定位置
     * @return 指定位置的值.
     */
    public Boolean[] getBulk(int... indexes) {
        List<Boolean> results = pool.allowingSlaves().transactionallyDo(p -> {
            for (int index : indexes) {
                get(p, index);
            }
        });
        return results.toArray(new Boolean[indexes.length]);
    }

    @Override
    public void set(int bitIndex, boolean value) {
        pool.safelyDo(jedis -> jedis.setbit(name, bitIndex, value));
    }


    public void get(Pipeline p, int position) {
        p.getbit(name, position);
    }

    /**
     * 使用管道执行{@link #set(int, boolean)}
     *
     * @param p        管道
     * @param bitIndex bit位置
     * @param value    待设置的值
     */
    public void set(Pipeline p, int bitIndex, boolean value) {
        p.setbit(name, bitIndex, value);
    }

    @Override
    public void set(int bitIndex) {
        set(bitIndex, true);
    }

    @Override
    public void clear(int bitIndex) {
        set(bitIndex, false);
    }

    @Override
    public void clear() {
        pool.safelyDo(jedis -> {
            jedis.del(name);
        });
    }

    @Override
    public int cardinality() {
        return pool.safelyReturn(jedis -> jedis.bitcount(name)).intValue();
    }


    @Override
    public int size() {
        return size;
    }


    @Override
    public byte[] toByteArray() {
        return pool.allowingSlaves().safelyReturn(jedis -> {
            byte[] bytes = jedis.get(SafeEncoder.encode(name));
            if (bytes == null) {
                //prevent null values
                bytes = new byte[(int) Math.ceil(size / 8)];
            }
            return bytes;
        });
    }

    /**
     * 转换成BitSet.
     *
     * @return BitSet.
     */
    public BitSet asBitSet() {
        return fromByteArrayReverse(toByteArray());
    }


    /**
     * 用BitSet的内容覆盖RedisBitSet
     *
     * @param bits BitSet
     */
    public void overwriteBitSet(BitSet bits) {
        pool.safelyDo(jedis -> jedis.set(SafeEncoder.encode(name), toByteArrayReverse(bits)));
    }

    @Override
    public String toString() {
        return asBitSet().toString();
    }

    public String getRedisKey() {
        return name;
    }

    /**
     * 是否给定位置上的值为true
     *
     * @param positions 给定位置
     * @return 是否给定位置上的值为true
     */
    public boolean isAllSet(int... positions) {
        Boolean[] results = getBulk(positions);
        return Stream.of(results).allMatch(b -> b);
    }

    /**
     * 批量设置
     *
     * @param positions 给定位置
     * @return 是否设置成功
     */
    public boolean setAll(int... positions) {
        List<Object> results = pool.transactionallyDo(p -> {
            for (int position : positions)
                p.setbit(name, position, true);
        });
        return results.stream().anyMatch(b -> !(Boolean) b);
    }

    //拷贝自: https://github.com/xetorthio/jedis/issues/301
    public static BitSet fromByteArrayReverse(final byte[] bytes) {
        final BitSet bits = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[i / 8] & (1 << (7 - (i % 8)))) != 0) {
                bits.set(i);
            }
        }
        return bits;
    }

    //拷贝自: https://github.com/xetorthio/jedis/issues/301
    public static byte[] toByteArrayReverse(final BitSet bits) {
        final byte[] bytes = new byte[bits.length() / 8 + 1];
        for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
                final int value = bytes[i / 8] | (1 << (7 - (i % 8)));
                bytes[i / 8] = (byte) value;
            }
        }
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RedisBitSet) obj = ((RedisBitSet) obj).asBitSet();
        return asBitSet().equals(obj);
    }
}
