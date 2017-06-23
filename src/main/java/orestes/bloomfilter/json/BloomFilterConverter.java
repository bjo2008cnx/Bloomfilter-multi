package orestes.bloomfilter.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.BloomFilterMemory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.BitSet;

public class BloomFilterConverter {

    /**
     * 转换 non-counting Bloom filter.为JSon格式
     *
     * @param source to convert
     * @return the JSON
     */
    public static JsonElement toJson(BloomFilter<?> source) {
        JsonObject root = new JsonObject();
        root.addProperty("m", source.getSize());
        root.addProperty("h", source.getHashes());
        //root.addProperty("HashMethod", source.config().hashMethod().name());
        byte[] bits = source.getBitSet().toByteArray();

        // Encode using Arrays.toString -> [0,16,0,0,32].
        // root.addProperty("bits", Arrays.toString(bits));

        // Encode using base64 -> AAAAAQAAQAAAAAAgA
        root.addProperty("b", toBase64(bits));

        return root;
    }

    /**
     * 转换 Bloom filter 为包含bits的bsse64 格式的字符串
     *
     * @param source  to convert
     * @return bsse64 格式的字符串
     */
    public static String toBase64(BloomFilter<?> source) {
        return toBase64(source.getBitSet().toByteArray());
    }

    private static String toBase64(byte[] bits) {
        return new String(Base64.getEncoder().encode(bits), StandardCharsets.UTF_8);
    }

    /**
     * 从json中加载
     *
     * @param source JSON 串
     * @return 转换后的Bloom filter
     */
    public static BloomFilter<String> fromJson(JsonElement source) {
        return fromJson(source, String.class);
    }

    /**
     * 从json中加载
     *
     * @param source JSON 串
     * @param type  类型
     * @param <T>  Bloom filter中元素的类型
     * @return the Bloom filter
     */
    public static <T> BloomFilter<T> fromJson(JsonElement source, Class<T> type) {
        JsonObject root = source.getAsJsonObject();
        int m = root.get("m").getAsInt();
        int k = root.get("h").getAsInt();
        //String hashMethod = root.get("HashMethod").getAsString();
        byte[] bits = Base64.getDecoder().decode(root.get("b").getAsString());

        FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3KirschMitzenmacher);

        BloomFilterMemory<T> filter = new BloomFilterMemory<>(builder.complete());
        filter.setBitSet(BitSet.valueOf(bits));

        return filter;
    }


}
