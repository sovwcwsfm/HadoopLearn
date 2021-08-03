2、需求
		经典案例：求出每一个订单中成交金额最大的一笔交易

示例数据如下：
| 订单编号  | 商品编号  | 金额 |
| --------- | --------- | ---- |
| order_001 | goods_001 | 100  |
| order_001 | goods_002 | 200  |
| order_002 | goods_003 | 300  |
| order_002 | goods_004 | 400  |
| order_002 | goods_005 | 500  |
| order_003 | goods_001 | 100  |

预期结果：
order_001	goods_002	200
order_002	goods_005	500
order_003	goods_001	100

步骤：
	1、定义实体类
		有最值或者比较的情况，想到WritableComparable
	2、定义Mapper
		map方法
	3、定义分区
		//参考源码  return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	4、定义分组
		 * 实现分组有固定的步骤：
		 * 1、继承WritableComparator
		 * 2、调用父类的构造器
		 * 3、指定分组的规则，重写一个方法
	5、定义Reducer
		reduce方法
	6、定义主类
		入口