package simple;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * <p>
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
/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Tests for MemoryBloomFilter.java
 *
 * @author Magnus Skjegstad <magnus@skjegstad.com>
 */
public class MemoryBloomFilterTest {
    static Random r = new Random();

    @Test
    public void testConstructorCNK() throws Exception {
        System.out.println("MemoryBloomFilter(c,n,k)");

        for (int i = 0; i < 10000; i++) {
            double c = r.nextInt(20) + 1;
            int n = r.nextInt(10000) + 1;
            int k = r.nextInt(20) + 1;
            MemoryBloomFilter bf = new MemoryBloomFilter(c, n, k);
            assertEquals(bf.getK(), k);
            assertEquals(bf.getExpectedBitsPerElement(), c, 0);
            assertEquals(bf.getExpectedNumberOfElements(), n);
            assertEquals(bf.size(), c * n, 0);
        }
    }


    /**
     * Test of createHash method, of class MemoryBloomFilter.
     *
     * @throws Exception
     */
    @Test
    public void testCreateHash_String() throws Exception {
        System.out.println("createHash");
        String val = UUID.randomUUID().toString();
        int result1 = MemoryBloomFilter.createHash(val);
        int result2 = MemoryBloomFilter.createHash(val);
        assertEquals(result2, result1);
        int result3 = MemoryBloomFilter.createHash(UUID.randomUUID().toString());
        assertNotSame(result3, result2);

        int result4 = MemoryBloomFilter.createHash(val.getBytes("UTF-8"));
        assertEquals(result4, result1);
    }

    /**
     * Test of createHash method, of class MemoryBloomFilter.
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCreateHash_byteArr() throws UnsupportedEncodingException {
        System.out.println("createHash");
        String val = UUID.randomUUID().toString();
        byte[] data = val.getBytes("UTF-8");
        int result1 = MemoryBloomFilter.createHash(data);
        int result2 = MemoryBloomFilter.createHash(val);
        assertEquals(result1, result2);
    }

    /**
     * Test of createHash method, of class MemoryBloomFilter.
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCreateHashes_byteArr() throws UnsupportedEncodingException {
        System.out.println("createHashes");
        String val = UUID.randomUUID().toString();
        byte[] data = val.getBytes("UTF-8");
        int[] result1 = MemoryBloomFilter.createHashes(data, 10);
        int[] result2 = MemoryBloomFilter.createHashes(data, 10);
        assertEquals(result1.length, 10);
        assertEquals(result2.length, 10);
        assertArrayEquals(result1, result2);
        int[] result3 = MemoryBloomFilter.createHashes(data, 5);
        assertEquals(result3.length, 5);
        for (int i = 0; i < result3.length; i++)
            assertEquals(result3[i], result1[i]);

    }

    /**
     * Test of equals method, of class MemoryBloomFilter.
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testEquals() throws UnsupportedEncodingException {
        System.out.println("equals");
        MemoryBloomFilter instance1 = new MemoryBloomFilter(1000, 100);
        MemoryBloomFilter instance2 = new MemoryBloomFilter(1000, 100);

        for (int i = 0; i < 100; i++) {
            String val = UUID.randomUUID().toString();
            instance1.add(val);
            instance2.add(val);
        }

        assert (instance1.equals(instance2));
        assert (instance2.equals(instance1));

        instance1.add("Another entry"); // make instance1 and instance2 different before clearing

        instance1.clear();
        instance2.clear();

        assert (instance1.equals(instance2));
        assert (instance2.equals(instance1));

        for (int i = 0; i < 100; i++) {
            String val = UUID.randomUUID().toString();
            instance1.add(val);
            instance2.add(val);
        }

        assertTrue(instance1.equals(instance2));
        assertTrue(instance2.equals(instance1));
    }

    /**
     * Test of hashCode method, of class MemoryBloomFilter.
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testHashCode() throws UnsupportedEncodingException {
        System.out.println("hashCode");

        MemoryBloomFilter instance1 = new MemoryBloomFilter(1000, 100);
        MemoryBloomFilter instance2 = new MemoryBloomFilter(1000, 100);

        assertTrue(instance1.hashCode() == instance2.hashCode());

        for (int i = 0; i < 100; i++) {
            String val = UUID.randomUUID().toString();
            instance1.add(val);
            instance2.add(val);
        }

        assertTrue(instance1.hashCode() == instance2.hashCode());

        instance1.clear();
        instance2.clear();

        assertTrue(instance1.hashCode() == instance2.hashCode());

        instance1 = new MemoryBloomFilter(100, 10);
        instance2 = new MemoryBloomFilter(100, 9);
        assertFalse(instance1.hashCode() == instance2.hashCode());

        instance1 = new MemoryBloomFilter(100, 10);
        instance2 = new MemoryBloomFilter(99, 9);
        assertFalse(instance1.hashCode() == instance2.hashCode());

        instance1 = new MemoryBloomFilter(100, 10);
        instance2 = new MemoryBloomFilter(50, 10);
        assertFalse(instance1.hashCode() == instance2.hashCode());
    }

    /**
     * Test of expectedFalsePositiveProbability method, of class MemoryBloomFilter.
     */
    @Test
    public void testExpectedFalsePositiveProbability() {
        // These probabilities are taken from the bloom filter probability table at
        // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
        System.out.println("expectedFalsePositiveProbability");
        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);
        double expResult = 0.00819; // m/n=10, k=7
        double result = instance.expectedFalsePositiveProbability();
        assertEquals(instance.getK(), 7);
        assertEquals(expResult, result, 0.000009);

        instance = new MemoryBloomFilter(100, 10);
        expResult = 0.00819; // m/n=10, k=7
        result = instance.expectedFalsePositiveProbability();
        assertEquals(instance.getK(), 7);
        assertEquals(expResult, result, 0.000009);

        instance = new MemoryBloomFilter(20, 10);
        expResult = 0.393; // m/n=2, k=1
        result = instance.expectedFalsePositiveProbability();
        assertEquals(1, instance.getK());
        assertEquals(expResult, result, 0.0005);

        instance = new MemoryBloomFilter(110, 10);
        expResult = 0.00509; // m/n=11, k=8
        result = instance.expectedFalsePositiveProbability();
        assertEquals(8, instance.getK());
        assertEquals(expResult, result, 0.00001);
    }

    /**
     * Test of clear method, of class MemoryBloomFilter.
     */
    @Test
    public void testClear() {
        System.out.println("clear");
        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);
        for (int i = 0; i < instance.size(); i++)
            instance.setBit(i, true);
        instance.clear();
        for (int i = 0; i < instance.size(); i++)
            assertSame(instance.getBit(i), false);
    }

    /**
     * Test of add method, of class MemoryBloomFilter.
     *
     * @throws Exception
     */
    @Test
    public void testAdd() throws Exception {
        System.out.println("add");
        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);

        for (int i = 0; i < 100; i++) {
            String val = UUID.randomUUID().toString();
            instance.add(val);
            assert (instance.contains(val));
        }
    }

    /**
     * Test of addAll method, of class MemoryBloomFilter.
     *
     * @throws Exception
     */
    @Test
    public void testAddAll() throws Exception {
        System.out.println("addAll");
        List<String> v = new ArrayList<String>();
        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);

        for (int i = 0; i < 100; i++)
            v.add(UUID.randomUUID().toString());

        instance.addAll(v);

        for (int i = 0; i < 100; i++)
            assert (instance.contains(v.get(i)));
    }

    /**
     * Test of contains method, of class MemoryBloomFilter.
     *
     * @throws Exception
     */
    @Test
    public void testContains() throws Exception {
        System.out.println("contains");
        MemoryBloomFilter instance = new MemoryBloomFilter(10000, 10);

        for (int i = 0; i < 10; i++) {
            instance.add(Integer.toBinaryString(i));
            assert (instance.contains(Integer.toBinaryString(i)));
        }

        assertFalse(instance.contains(UUID.randomUUID().toString()));
    }

    /**
     * Test of containsAll method, of class MemoryBloomFilter.
     *
     * @throws Exception
     */
    @Test
    public void testContainsAll() throws Exception {
        System.out.println("containsAll");
        List<String> v = new ArrayList<String>();
        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);

        for (int i = 0; i < 100; i++) {
            v.add(UUID.randomUUID().toString());
            instance.add(v.get(i));
        }

        assert (instance.containsAll(v));
    }

    /**
     * Test of getBit method, of class MemoryBloomFilter.
     */
    @Test
    public void testGetBit() {
        System.out.println("getBit");
        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);
        Random r = new Random();

        for (int i = 0; i < 100; i++) {
            boolean b = r.nextBoolean();
            instance.setBit(i, b);
            assertSame(instance.getBit(i), b);
        }
    }

    /**
     * Test of setBit method, of class MemoryBloomFilter.
     */
    @Test
    public void testSetBit() {
        System.out.println("setBit");

        MemoryBloomFilter instance = new MemoryBloomFilter(1000, 100);
        Random r = new Random();

        for (int i = 0; i < 100; i++) {
            instance.setBit(i, true);
            assertSame(instance.getBit(i), true);
        }

        for (int i = 0; i < 100; i++) {
            instance.setBit(i, false);
            assertSame(instance.getBit(i), false);
        }
    }

    /**
     * Test of size method, of class MemoryBloomFilter.
     */
    @Test
    public void testSize() {
        System.out.println("size");
        for (int i = 100; i < 1000; i++) {
            MemoryBloomFilter instance = new MemoryBloomFilter(i, 10);
            assertEquals(instance.size(), i);
        }
    }

    /**
     * Test error rate *
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testFalsePositiveRate1() throws UnsupportedEncodingException {
        // Numbers are from // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
        System.out.println("falsePositiveRate1");

        for (int j = 10; j < 21; j++) {
            System.out.print(j - 9 + "/11");
            List<byte[]> v = new ArrayList<byte[]>();
            MemoryBloomFilter instance = new MemoryBloomFilter(100 * j, 100);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = new byte[100];
                r.nextBytes(bytes);
                v.add(bytes);
            }
            instance.addAll(v);

            long f = 0;
            double tests = 300000;
            for (int i = 0; i < tests; i++) {
                byte[] bytes = new byte[100];
                r.nextBytes(bytes);
                if (instance.contains(bytes)) {
                    if (!v.contains(bytes)) {
                        f++;
                    }
                }
            }

            double ratio = f / tests;

            System.out.println(" - got " + ratio + ", math says " + instance.expectedFalsePositiveProbability());
            assertEquals(instance.expectedFalsePositiveProbability(), ratio, 0.01);
        }
    }

    /**
     * Test for correct k
     **/
    @Test
    public void testGetK() {
        // Numbers are from http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
        System.out.println("testGetK");
        MemoryBloomFilter instance = null;

        instance = new MemoryBloomFilter(2, 1);
        assertEquals(1, instance.getK());

        instance = new MemoryBloomFilter(3, 1);
        assertEquals(2, instance.getK());

        instance = new MemoryBloomFilter(4, 1);
        assertEquals(3, instance.getK());

        instance = new MemoryBloomFilter(5, 1);
        assertEquals(3, instance.getK());

        instance = new MemoryBloomFilter(6, 1);
        assertEquals(4, instance.getK());

        instance = new MemoryBloomFilter(7, 1);
        assertEquals(5, instance.getK());

        instance = new MemoryBloomFilter(8, 1);
        assertEquals(6, instance.getK());

        instance = new MemoryBloomFilter(9, 1);
        assertEquals(6, instance.getK());

        instance = new MemoryBloomFilter(10, 1);
        assertEquals(7, instance.getK());

        instance = new MemoryBloomFilter(11, 1);
        assertEquals(8, instance.getK());

        instance = new MemoryBloomFilter(12, 1);
        assertEquals(8, instance.getK());
    }

    /**
     * Test of contains method, of class MemoryBloomFilter.
     */
    @Test
    public void testContains_GenericType() {
        System.out.println("contains");
        int items = 100;
        MemoryBloomFilter<String> instance = new MemoryBloomFilter(0.01, items);

        for (int i = 0; i < items; i++) {
            String s = UUID.randomUUID().toString();
            instance.add(s);
            assertTrue(instance.contains(s));
        }
    }

    /**
     * Test of contains method, of class MemoryBloomFilter.
     */
    @Test
    public void testContains_byteArr() {
        System.out.println("contains");

        int items = 100;
        MemoryBloomFilter instance = new MemoryBloomFilter(0.01, items);

        for (int i = 0; i < items; i++) {
            byte[] bytes = new byte[500];
            r.nextBytes(bytes);
            instance.add(bytes);
            assertTrue(instance.contains(bytes));
        }
    }

    /**
     * Test of count method, of class MemoryBloomFilter.
     */
    @Test
    public void testCount() {
        System.out.println("count");
        int expResult = 100;
        MemoryBloomFilter instance = new MemoryBloomFilter(0.01, expResult);
        for (int i = 0; i < expResult; i++) {
            byte[] bytes = new byte[100];
            r.nextBytes(bytes);
            instance.add(bytes);
        }
        int result = instance.count();
        assertEquals(expResult, result);

        instance = new MemoryBloomFilter(0.01, expResult);
        for (int i = 0; i < expResult; i++) {
            instance.add(UUID.randomUUID().toString());
        }
        result = instance.count();
        assertEquals(expResult, result);
    }


}