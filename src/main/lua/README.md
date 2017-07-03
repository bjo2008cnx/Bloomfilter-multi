这是有lua实现的方案，跟java实现方案二选一


redis-lua-scaling-bloom-filter
==============================

`add.lua`, `cas.lua` 和 `check.lua`是 [scaling bloom filter](http://en.wikipedia.org/wiki/Bloom_filter#Scalable_Bloom_filters) for [Redis](http://redis.io/)的脚本

`layer-add.lua` 和 `later-check.lua` 是 [scaling layered bloom filter](https://en.wikipedia.org/wiki/Bloom_filter#Layered_Bloom_filters) for [Redis](http://redis.io/)的脚本

可使用redis的 [EVAL](http://redis.io/commands/eval) 命令执行

在redis集群下可能无法使用，因为 脚本中的key没有全部作为参数进行传递(since the keys used inside the script aren't all passed as arguments!)
Layered_Bloom_filter最大可以有32层，可以源码中进行修改

`add.lua`, `cas.lua` and `layer-add.lua`
----------------------------------------

add.lua` 用于增加新元素. 如果bloomfilter不存在，自动创建

cas.lua` Check And Set,如果元素已存在则不再添加,并返回1，否则进行添加然后返回0

由于我们使用缩放过滤器，使用`add.lua添加元素可能会导致元素同时存在于过滤器的多个部分。 `cas.lua`可以避免这个问题。
使用`cas.lua`过滤器的`：count`键可以准确计算添加到过滤器中的元素数量, 使用“cas.lua”可以降低错误率（更少的重复次数意味着更少的bit被重复设置）。

`layer-add.lua`与`cas.lua`做类似的事情，它需要检查层中的所有过滤器，以查看它是否已经存在于层中。
`layer-add.lua`将返回添加元素的层号。

这些脚本期望有4个参数。

1.要使用的键的名称。
2.BloomFilter的初始大小（以元素数量计）。
3.误报的概率。
4.待添加到过滤器的元素。


例如，以下调用将向名为test的BloomFilter添加“something”,最初可以容纳10000个元素，误报率为1％。
eval "add.lua source here" 0 test 10000 0.01 something

eval "add.lua source here" 0 test 100000 0.01 something

`check.lua` and `layer-check.lua`
---------------------------------

`check.lua` & `layer-check.lua` 脚本检查一个元素是否包含在bloom过滤器中,`layer-check.lua`返回元素所在的层。

这些脚本期望有4个参数。

1.要使用的键的名称。
2.过滤器的初始大小（以元素数量计）。
3.误报的概率。
4.要检查的元素。

例如，以下调用将检查“something”是否为名为test的过滤器的一部分,最初可以容纳10000个元素，误报率为1％。
eval "check.lua source here" 0 test 10000 0.01 something
`


测试情况
-----

```
$ npm install redis srand
$ node add.js
$ node cas.js
$ node check.js
$ # or/and
$ node layer-add.js
$ node layer-check.js
```
`add.js`和`layer-add.js`会将元素添加到名为test的过滤器中，然后检查元素是否是过滤器的一部分。
`check.js`和`layer-check.js`将根据`add.js`或`layer-add.js'的过滤器生成测试随机元素，以发现错误的概率。

基于假设Redis在默认端口上运行。

Benchmark(基准测试)
---------

您可以运行`./ benchmark.sh`和`./ layer-benchmark.sh`查看脚本的速度。

这个脚本假设Redis在默认端口上运行，并安装了`redis-cli`和`redis-benchmark`

这是我的2.3GHz 2012 MacBook Pro Retina的输出：

```
add.lua
====== evalsha ab31647b3931a68b3b93a7354a297ed273349d39 0 HSwVBmHECt 1000000 0.01 :rand:000000000000 ======
  200000 requests completed in 8.27 seconds
  20 parallel clients
  3 bytes payload
  keep alive: 1

94.57% <= 1 milliseconds
100.00% <= 2 milliseconds
24175.03 requests per second


check.lua
====== evalsha 437a3b0c6a452b5f7a1f10487974c002d41f4a04 0 HSwVBmHECt 1000000 0.01 :rand:000000000000 ======
  200000 requests completed in 8.54 seconds
  20 parallel clients
  3 bytes payload
  keep alive: 1

92.52% <= 1 milliseconds
100.00% <= 8 milliseconds
23419.20 requests per second


layer-add.lua
====== evalsha 7ae29948e3096dd064c22fcd8b628a5c77394b0c 0 ooPb5enskU 1000000 0.01 :rand:000000000000 ======
  20000 requests completed in 12.61 seconds
  20 parallel clients
  3 bytes payload
  keep alive: 1

55.53% <= 12 milliseconds
75.42% <= 13 milliseconds
83.71% <= 14 milliseconds
91.48% <= 15 milliseconds
97.76% <= 16 milliseconds
99.90% <= 24 milliseconds
100.00% <= 24 milliseconds
1586.04 requests per second


layer-check.lua
====== evalsha c1386438944daedfc4b5c06f79eadb6a83d4b4ea 0 ooPb5enskU 1000000 0.01 :rand:000000000000 ======
  20000 requests completed in 11.13 seconds
  20 parallel clients
  3 bytes payload
  keep alive: 1

0.00% <= 9 milliseconds
74.12% <= 11 milliseconds
80.43% <= 12 milliseconds
83.93% <= 13 milliseconds
97.43% <= 14 milliseconds
99.89% <= 15 milliseconds
100.00% <= 15 milliseconds
1797.59 requests per second
```

