v0.1.3 20181103 
1、修复同一个表多个配置文件的工作流，运行的session日志会相互覆盖的BUG；

v0.1.2 20180902 
1、新增按时间字段只增改类型的mapping，代码2.2

v0.1.1 20180902 
1、针对目标表表名不能通过截取的方式获取的情况，进行优化，支持从数据库表取对应关系；

v0.1.0 20180902 
1、表名文件中的表名支持带用户名称，方便生成同一数据库不同用户下的表；
2、一个表支持生成不同配置文件的多个工作流,详见infa.xml配置参数workflow.muti-params的说明；
3、增量mapping生成时发现表没有主键，支持直接生成全删全插的流程，不需要在单独重新生成，控制参数为AddIfIncError，默认NO
3、增加mapping类型2.1，功能为：增量比对并写日志到日志表（ETL_DELETE_LOG）。特别注意，需要提前导入ETL_DELETE_LOG表，因为ETL_DELETE_LOG表是所有表共用的。导入方式：手工建立或者导入ETL_DELETE_LOG.XML
4、修改mapping类型3，功能为：正常全删全插，其他大于3的则为带HY_ID的全删全插（HY_ID取第一个字段）;



v0.0.2
修正：
1、唯一索引中含有PK的优先级提高，更能识别为主键
2、源表中如果存在QYSJ\JLSJ\JLZT等字段可以通过配置文件配置新的字段
v0.0.1
修正：
1、可以识别唯一索引当主键，随机取字符排序最大的唯一索引