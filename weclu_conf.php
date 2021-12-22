<?php
define( 'WECLU_SYSTEM_PATH', dirname(__FILE__) ) ;	//数据库系统路径
define( 'WECLU_BASE_PATH', WECLU_SYSTEM_PATH . '/db/' );	//数据库数据路径
define( 'WECLU_CORE_PATH', WECLU_SYSTEM_PATH . '/core/' );	//数据库核心程序路径
define( 'WECLU_INDEX_LEVEL', 4 ) ;			//索引级别
define( 'WECLU_IS_OPEN_THREAD', false ) ;			//是否开启多线程
define( 'WECLU_TABLE_FILE_MAX_BLOCKS', 157286400 ) ;	//数据表大小最大含有的文件快个数
define( 'WECLU_DATA_COLLECT_STAMP', 604800 ) ;		//数据回收时间 7天时间
define( 'WECLU_DATA_COLLECT_PERCENT', 0.1 ) ;		//数据回收时有效数据百分比
define( 'WECLU_DATATYPE_SIZE', 1 ) ;	//数据类型占用空间长度
define( 'WECLU_DATABASE_TYPE', 0.1 ) ;
define( 'WECLU_HEADER_BLOCK_SIZE', 1024 ) ;     //存放主表头部信息空间字节数

//HASH存储引擎
define( 'WECLU_BLOCK_SIZE', 100 ) ;		//数据块大小
define( 'WECLU_KEY_SIZE', 50 ) ;		//数据键字符最大长度
define( 'WECLU_HEAD_BLOCK_SIZE', 12 ) ;		//数据键字符最大长度 最大支持100G 单文件大小

//RDS存储引擎
define( 'WECLU_RDS_BLOCK_SIZE', 100 ) ;		//数据RDS块大小
define( 'WECLU_RDS_ID_LENGTH', 9 ) ;    // 最大支持15位整数
define( 'WECLU_RDS_MAP_ADDR', 8 ) ;    //数据分表长度， 支持数据分表数 
define( 'WECLU_RDS_DATA_BLOCKS', 5 ) ;    //一条数据占用最大数据块 60466175

//hash 
define( 'WECLU_HASH_BLOCK_SIZE', 1024 );    //hash主表块的长度， 此处最好是1024的整数倍
define( 'WECLU_HASH_HEAD_BLOCK_SIZE', 8 );    //hash主表块的块头长度 8字节
define( 'WECLU_HASH_ASSIST_BLOCK_SIZE', WECLU_HASH_BLOCK_SIZE * 1 ); //hash辅助数据块的长度
define( 'WECLU_HASH_BLOCK_RULES', base_convert( 'fffff', 16, 10 ) * WECLU_HASH_BLOCK_SIZE );    //hash主空间分割位置

//btree
define( 'WECLU_BTREE_HEADER_BLOCK_SIZE', 1024 );    //索引头部信息
define( 'WECLU_BTREE_BLOCK_SIZE', 1024 );           //索引块信息
define( 'WECLU_BTREE_KEY_BLOCK_SIZE', 32 );         //索引键值块大小
define( 'WECLU_BTREE_POS_BLOCK_SIZE', 8 );
define( 'WECLU_BTREE_ASSIST_BLOCK_SIZE', 1024 );
define( 'WECLU_BTREE_MAX_KEY_SIZE', WECLU_BTREE_KEY_BLOCK_SIZE + WECLU_BTREE_POS_BLOCK_SIZE + 6 );
