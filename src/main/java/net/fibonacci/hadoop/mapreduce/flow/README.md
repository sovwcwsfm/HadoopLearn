MapReduce项目实战

需求：流量统计经典案例

| 时间戳     | ts            | long   |
| 手机号     | phone         | String |
| 基站编号   | id            | String |
| IP         | Ip            | String |
| URL        | url           | String |
| URL类型    | Type          | String |
| 上行数据包 | upFlow        | int    |
| 下行数据包 | downFlow      | int    |
| 上行流量   | upCountFlow   | int    |
| 下行流量   | downCountFlow | int    |
| 响应       | status        | String |

1363157995033 	15920133257	5C-0E-8B-C7-BA-20:CMCC	120.197.40.4	sug.so.360.cn	信息安全	20	        20	        3156	 2936	  200
时间戳			手机号 		基站编号 				IP				URL 			URL类型		上行数据包  下行数据包  上行流量 下行流量 响应

需求一：统计每个手机号的数据包和流量总和
思路：  1、定义实体类
		2、定义FlowCountMapper
		3、定义FlowCountReducer
		4、主调度入口

需求二：将需求一中结果按照upFlow流量倒排
思路
    1、定义实体类 手机号 + upFlow = FlowSortBean
    2、定义FlowSortMapper key-> FlowSortBean value -> null
    3. 定义FlowSortReducer key-> FlowSortBean value -> null
    4. 主调度入口
需求三：手机号码分区
	 135 开头放一个文件
	 136 开头放一个文件
	 137 开头放一个文件
	 其他开头   放一个文件
