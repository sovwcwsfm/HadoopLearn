需求：数据格式如下，要求第一列按照字典顺序进行排列，第一列相同的时候, 第二列按照升序进行排列。
	a 1
	a 3
	b 1
	a 2
	c 2
	c 1

	预期结果如下：
	a 1
	a 2
	a 3
	b 1
	c 1
	c 2

思路：
	1、将 Mapper 端输出的<key,value>中的 key 和 value 组合成一个新的 key , value值不变，也就是新的key和value为：<(key,value),value>
	2、在针对新的 key 排序的时候, 如果 key 相同, 就再对value进行排序