<?php
define( 'ZJRDB_BASE_PATH', './db/' ) ;	//数据库数据路径
define( 'ZJRDB_INDEX_TYPE', 4 ) ;			//索引级别
define( 'ZJRDB_IS_OPEN_THREAD', false ) ;			//是否开启多线程
define( 'ZJRDB_TABLE_FILE_MAX_BLOCKS', 157286400 ) ;	//数据表大小最大含有的文件快个数
define( 'ZJRDB_DATA_COLLECT_STAMP', 604800 ) ;		//数据回收时间 7天时间
define( 'ZJRDB_DATA_COLLECT_PERCENT', 0.1 ) ;		//数据回收时有效数据百分比
define( 'ZJRDB_DATATYPE_SIZE', 1 ) ;	//数据类型占用空间长度
define( 'ZJRDB_DATABASE_TYPE', 0.1 ) ;
define( 'ZJRDB_HEADER_BLOCK_SIZE', 1024 ) ;     //存放主表头部信息空间字节数

//NOSQL存储引擎
define( 'ZJRDB_BLOCK_SIZE', 100 ) ;		//数据块大小
define( 'ZJRDB_KEY_SIZE', 50 ) ;		//数据键字符最大长度
define( 'ZJRDB_HEAD_BLOCK_SIZE', 12 ) ;		//数据键字符最大长度 最大支持100G 单文件大小

//RDS存储引擎
define( 'ZJRDB_RDS_BLOCK_SIZE', 100 ) ;		//数据RDS块大小
define( 'ZJRDB_RDS_ID_LENGTH', 9 ) ;    // 最大支持15位整数
define( 'ZJRDB_RDS_MAP_ADDR', 8 ) ;    //数据分表长度， 支持数据分表数 
define( 'ZJRDB_RDS_DATA_BLOCKS', 5 ) ;    //一条数据占用最大数据块 60466175
/*
's_id'  => 0,       //起始ID
'n_id'  => 0,       //最新ID
'lock'  => 0,       //是否上锁
'type'  => $type,   //类型
'val_nums' => 0,    //有效数据数
'inval_min' => 0,   //无效数据最小ID
'inval_max' => 0    //无效数据最大ID
 */