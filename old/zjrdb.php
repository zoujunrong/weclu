<?php
/**
 * @description 文件数据库类
 * @author zjr
 * @create time 2015/10/29
 * @V2.0
 * @update time 2016/3/07
 * @description
 * V1.0 说明： key-val 数据库，文件每次都是全部读全部写，建议不同功能指定不同空间和不同表
 * V2.0 优化支持海量数据高效读取，支持事务，主要用于统计类, 日志类场景
 * 测试效率： 查询：耗时一秒查询支持数据量， 740000 * 16^索引级别， 支持32级索引。 插入：单条插入速度：0.0060s
 * 
 * V3.0 
 * 1、支持空间重复利用
 * 2、支持排序
 * 3、数据存储结构大改
 * 4、索引结构变更
 */
include 'zjrdb_conf.php';
define( 'ZJRDB_C', 1 ) ;	//增
define( 'ZJRDB_R', 2 ) ;	//查
define( 'ZJRDB_U', 3 ) ;	//改
define( 'ZJRDB_D', 4 ) ;	//删
class ZJRDB {
	private $base_path = ZJRDB_BASE_PATH;
	private $trans_path;
	private $filter_path;
	private $data_index_path;
	private $number_index_path;
	private $index_path;
	private $index_type = ZJRDB_INDEX_TYPE;
	private $db_type = 'nosql';
    private $conf_max_size = 1023;	//1023 byte
    private $table_file_max_size = ZJRDB_TABLE_FILE_MAX_BLOCKS;	//150M
    private $issetHead = false;
    private $file_config = array();
    private $collect_stamp = ZJRDB_DATA_COLLECT_STAMP; //7天时间
    private $space = '';
    private $table = '';
    private $handle = null;
    private $data_handle = null;
    private $file_path;
    private $file_data_path;    //数据空间
    private $table_suffix = '';
    private $data_table_suffix = '';
    private $trans_token = '';
    private $searchTreeData = array();	//缓存遍历树数据
    private $cacheIndexData = array();	//缓存索引数据
    private $seekData = array();	//查询数据
    private $searchCount = 0;
    private $dataCount = 0;
    private $searchKeyArr = array();	
    private $condition = array();
    private $isOpenThread = ZJRDB_IS_OPEN_THREAD;
	public function __construct($space, $table='') {
		if (empty($space)) return false;
		$this->space = $space;
		if (!empty($table)) {
			$this->table = $table;
			$this->init();
		}
	}
	
	/* public function __destruct() {
		$this->filterOverDateData();
	} */
	
	private function init() {
	    $this->trans_path = $this->base_path . '@trans/';
	    $this->filter_path = $this->base_path . '@filter/' . $this->space . '/';
	    $this->index_path = $this->base_path . '@index/' . $this->space . '/';
	    $this->data_index_path = $this->base_path . '@dataIndex/' . $this->space . '/' . $this->table . '/';
	    $this->number_index_path = $this->base_path .'@numberIndex/'. $this->space . '/' . $this->table. '/';
	    $this->file_path = $this->base_path .$this->space. '/'.$this->table.'/'.$this->table.$this->table_suffix.'.db';
	    
	    $tmp_dir = dirname($this->file_path);
	    if (!is_dir($tmp_dir)) {
	        $this->mkdirs($tmp_dir);
	    }
	    //创建事务文件夹
	    if (!is_dir($this->trans_path)) {
	    	$this->mkdirs($this->trans_path);
	    }
	    //创过滤引文件夹
	    if (!is_dir($this->filter_path)) {
	    	$this->mkdirs($this->filter_path);
	    }
	    //创建索引文件夹
	    if (!is_dir($this->index_path)) {
	    	$this->mkdirs($this->index_path);
	    }
	    //写入文件配置信息
	    $this->initConfigInfo();
	    
	}
	
	/**
	 * 创建表
	 * @param unknown $table
	 * @param string $type
	 */
	public function create($table, $type='NOSQL'){
		if (empty($table)) return false;
		if (!in_array($type, array('NOSQL', 'RDS'))) return false;
		$this->table = $table;
		$this->init();
		if (is_file($this->file_path) && filesize($this->file_path) > 0 ) {
			return false;
		}
		//创建数据索引文件夹
		if (!is_dir($this->data_index_path) && $type=='RDS') {
			$this->mkdirs($this->data_index_path);
		}
		//创建数字索引文件夹
		if ( ! is_dir( $this->number_index_path ) && $type=='RDS' ) {
			$this->mkdirs( $this->number_index_path );
		}
		
		$headStr = json_encode(
		    array(
		          's_id'  => 0,       //起始ID
		          'n_id'  => 0,       //最新ID
		          'lock'  => 0,       //是否上锁
		          'type'  => $type,   //类型
		          'val_nums' => 0,    //有效数据数
		          'inval_min' => 0,   //无效数据最小ID
		          'inval_max' => 0    //无效数据最大ID
		        )
		    );
		$left = $this->conf_max_size - strlen($headStr);
		
		file_put_contents($this->file_path, $headStr."\n");
		//写入文件配置信息
	    $this->initConfigInfo();
		return true;
	}
	
	/**
	 * 切换主分表
	 */
	private function changeTableSuffix( $suffix='', $mode='r' ) {
		$this->table_suffix = !empty($suffix) && $suffix != '@' ? $suffix : '';

		$this->file_path = $this->base_path .$this->space. '/'.$this->table.'/'.$this->table.$this->table_suffix.'.db';

		if (!is_file($this->file_path)) {
			$this->create($this->table, $this->file_config['type']);
		}
		if (!empty($this->handle)) {
			//关闭文件
			flock($this->handle, LOCK_UN);
			fclose($this->handle);
			$this->handle = fopen($this->file_path, $mode);
			$lock = ($mode=='r+') ? LOCK_EX : LOCK_SH;
			if (!empty($lock)) {
				flock($this->handle, $lock);
			}
		}
	}
	
	/**
	 * 切换表
	 * @param unknown $table
	 * @return boolean
	 */
	public function changeTable ($table) {
		if (empty($table)) false;
		$this->table = $table;
		$this->init();
		return true;
	}

	/**
	 * 获取所有数据库
	 */
	public function showDatabases() {
		$paths = glob($this->base_path.'*');
		$arr = array();
		foreach ($paths as $path) {
			$arr[] = basename($path);
		}
		return $arr;
	}

	/**
	 * 获取数据库所有表
	 */
	public function showTables() {
		$paths = glob($this->base_path.$this->space.'/*');
		$arr = array();
		foreach ($paths as $path) {
			$tmp = basename($path);
			if (strpos($tmp, '@') === false) $arr[] = $tmp;
		}
		return $arr;
	}
	
	/**
	 * 验证表名
	 * @param unknown $key
	 */
	private function checkTable($table, $type='') {
	}
	
	/**
	 * 验证键值
	 * @param unknown $key
	 */
	private function checkKey($key) {
	}
	
	/**
	 * 验证where条件
	 * @param unknown $key
	 */
	private function checkWhere($where) {
	}
	
	
	/**
	 * nosql方式查询数据
	 * @param unknown $key
	 * @param string $isVague
	 * @return multitype:
	 */
	public function get($key, $isVague = false) {
		if ($this->file_path) {
		    
			$this->handle = fopen($this->file_path, 'r');

			flock($this->handle, LOCK_SH);

			if ( ! $isVague ) {
				$data = $this->seekValByIndex( array( $key ) );
			} else {
				$data = $this->seekValByKey( $key );
			}
			
			flock($this->handle, LOCK_UN);
			fclose($this->handle);

			return isset($data[$key]) && !empty($data[$key]) ? ($data[$key]) : array();
		}
		return array();
	}
	
	/**
	 * nosql方式批量查询数据
	 * @param unknown $key
	 * @param string $isVague
	 * @return multitype:
	 */
	public function gets($keys) {

		if ($this->file_path) {
		    
			$this->handle = fopen($this->file_path, 'r');

			flock($this->handle, LOCK_SH);

			$data = $this->seekValByIndex( $keys );
			
			flock($this->handle, LOCK_UN);

			fclose($this->handle);

			return $data;
		}
		return array();
	}
	
	/**
	 * nosql方式设置数据
	 * @param unknown $key
	 * @param unknown $value
	 * @param number $expire
	 * @return boolean|multitype:number
	 */
	public function set($key, $value, $expire = 0) {
		if (empty($this->file_path) || $this->file_config['type'] != 'NOSQL') return false;
		$this->handle = fopen($this->file_path, 'r+');
		
		flock($this->handle, LOCK_EX);
		
		$res = $this->insert_core(array(array($key, $value, $expire)));

		flock($this->handle, LOCK_UN);
		fclose($this->handle);
		
		unset($this->handle);
		return $res;
	}
	
	/**
	 * 事务开启
	 */
	public function begin() {
		if ( empty( $this->trans_token ) ) 
			$this->trans_token = ( microtime(true) * 10000 ) . rand(100, 1000) . '.begin' ;
	}
	
	/**
	 * 事务提交
	 */
	public function commit() {
		if (empty($this->trans_token)) return false;
		$trans_file = $this->trans_path . $this->trans_token;
		//修改文件为提交状态
		if ( is_file( $trans_file ) ) {
			$this->trans_token .= '.commit'; 
			rename( $trans_file, $trans_file . '.commit' );
		}
		//开始写入数据
		$this->submitTransData();
		
		$trans_file = $this->trans_path . $this->trans_token;
		if ( is_file( $trans_file ) ) {
			unlink( $trans_file );
		}
		$this->trans_token = '';
		return true;
	}
	
	/**
	 * 事务回滚
	 */
	public function rollback() {
		if (empty($this->trans_token)) return false;
		if ( is_file( $this->trans_path . $this->trans_token ) ) {
			unlink( $this->trans_path . $this->trans_token );
		}
		$this->trans_token = '';
	}
	
	/**
	 * 记录事物数据
	 * @param $index 数据物理位置
	 * @param $data 数据
	 * @param $oper 操作  CRUD
	 */
	public function recordTransData( $index, $data, $oper ) {
		//数据域说明：  事物处理状态 | 操作状态 | 库 | 表 | 表后缀 | 数据库类型  | 数据  
		file_put_contents( $this->trans_path . $this->trans_token, '0|' . $oper . '|' . $this->space . '|' . $this->table . '|' . $this->table_suffix . '|' . $index . '|' . $this->file_config['type'] . '|' . base64_encode( $data ) . "\n" , FILE_APPEND );
	}
	
	/**
	 * 提交事物数据
	 */
	private function submitTransData() {
		$trans_handle = fopen( $this->trans_path . $this->trans_token, 'r+' );
		flock($trans_handle, LOCK_EX);
		$data = array();
		while ( ! feof( $trans_handle ) ) {
			//获取一行数据
			$line = fgets( $trans_handle );
			$data = explode( '|', $line );
			
			if ( ! empty( $data ) && $data[0] == '0' ) {
				
				$file_path = $this->base_path .$data[2]. '/'.$data[3].'/'.$data[3].$data[4].'.db';
				$handle = fopen($file_path, 'r+');
				flock( $handle, LOCK_EX );
				//开始写入数据
				fseek( $handle, $data[5] );
				$writeStr = base64_decode( $data[7] );
				fwrite( $handle, $writeStr, strlen( $writeStr ) );
				flock( $handle, LOCK_UN );
				fclose( $handle );
				//创建索引， 前提是第一条数据, 并且在新增数据时创建索引
				if ( substr( $writeStr, 0, 1 ) == '1' && $data[1] == ZJRDB_C ) {
					//如果是关系型数据库，则要创建数据索引
					if ( $data[6] == 'RDS' ) {
// 						$tmp = substr(strrchr($v['data'], '|'), 1);
// 						$this->createDataIndex($curTell, trim($tmp));
					} elseif ( $data[6] == 'NOSQL' ) {
						//非关系型插入成功开始创建索引
						$this->createIndex( array( trim( substr( $writeStr, ZJRDB_HEAD_BLOCK_SIZE + 1, ZJRDB_KEY_SIZE ) ) => $data[5] ) );
					}
				}
			}
			
		}
		flock($trans_handle, LOCK_UN);
		fclose($trans_handle);
	}
	
	/**
	 * 设置最新一张表
	 */
	private function setLatestTable() {
		if (filesize($this->file_path) >= $this->table_file_max_size) {
			$files = glob(dirname($this->file_path).'/*');
			$latestFile = end($files);
			if (filesize($latestFile) <= $this->table_file_max_size) {
				$fileName = basename($latestFile, '.db');
				$suffix = strstr($fileName,'@');
			} else {
				$suffix = '@'.date('YmdHis');
			}
			$this->changeTableSuffix($suffix, 'r+');
		}
	} 
	
	/**
	 * 头部改变时
	 */
	private function headerChange( $type='insert' ){
	    fseek( $this->handle, 0 );
	    fwrite($this->handle, json_encode( $this->file_config )."\n");
	}
	
    private function insert_core($keyValArr) {
		$nowTime = time();
		$result = array();
		if (!empty($keyValArr)) {
			foreach ($keyValArr as $v) {
			    $mallocs = array();
			    //查找数据是否存在
			    $exist = $this->getIndex(array($v[0]));
			    if ( !empty( $exist ) ) {
			        //定位表
			        $this->changeTableSuffix( key($exist), 'r+');
			        //将文件指针移到末尾
			        fseek($this->handle, key(end($exist)));
			        //获取原始数据空间
			        $mallocs = $this -> seekSourceSpace();
			    } else {
			        //重置表
			        $this->setLatestTable();
			        
			        //将文件指针移到末尾
			        fseek($this->handle, $this->file_config['n_id'] + ZJRDB_BLOCK_SIZE );
			    }
				if (empty($v) || empty($v[0])) continue;
				$invalidTime = empty($v[2]) ? 0 : ($nowTime + intval($v[2]));
				
				//先对数据进行压缩， 判断数据百分比 开始分配数据空间
				$d_z_m = ZJRDB_BLOCK_SIZE - ZJRDB_KEY_SIZE - ZJRDB_HEAD_BLOCK_SIZE - 13;  //主数据空间
				$d_z_s = $d_z_m + 10 + ZJRDB_KEY_SIZE;        //辅助数据空间
				$d_v = base64_encode( gzdeflate( $v[1] ) );     //压缩后的数据
				$d_l = strlen( $d_v );  //数据字节数
				$d_p_m = $d_l / $d_z_m ;  //主数据空间占用百分比
				
				//计算需要的空间块
				$d_z_s_n = 1;
				if ( $d_p_m > 1 ) {
				    $d_z_s_n += ceil( ( $d_l - $d_z_m ) / $d_z_s );
				}
				//计算需要再分配的空间数
				$source_space_count = count( $mallocs );
				if ( $source_space_count < $d_z_s_n ) {
				    //新增分配空间
				    $rellocs = $this -> seekFreeSpace( $d_z_s_n - $source_space_count );
				    $mallocs = array_merge( $mallocs, $rellocs );
				} 
				//如果需要的空间大于分配的空间，则需要释放多余的空间
				elseif ( $source_space_count > $d_z_s_n ) {
				    $free_space = array_slice( $mallocs, $d_z_s_n );
				    //释放空间
				    $this -> freeSpace( $free_space );
				}
				$writeArr = array();
				//对只有主数据空间的数据块
				if ( $d_z_s_n == 1 ) {
				    $tail_str = str_pad( 0, ZJRDB_HEAD_BLOCK_SIZE, ' ' ); 
				    $key_str = str_pad( $v[0], ZJRDB_KEY_SIZE, ' ' );
				    $expire_time_str = str_pad( $invalidTime, 10, ' ' );
				    $data_percent = str_pad( substr( ceil( $d_p_m * 100 ), -2, 2 ), 2, '0', STR_PAD_LEFT ); //数据占用空间的百分比
				    $writeArr[$mallocs[0]] = $tail_str . $key_str . $expire_time_str . $data_percent . str_pad( $d_v, $d_z_m, ' ' );
				} else {
				    //主数据段
				    $i = 0;
				    $tail_str = str_pad( $mallocs[$i+1], ZJRDB_HEAD_BLOCK_SIZE, ' ' );
				    $key_str = str_pad( $v[0], ZJRDB_KEY_SIZE, ' ' );
				    $expire_time_str = str_pad( $invalidTime, 10, ' ' );
				    $data_percent = substr( ceil( $d_p_m * 100 ), -2, 2 ); //数据占用空间的百分比
				    $writeArr[$mallocs[$i]] = $tail_str . $key_str . $expire_time_str . '00' . substr($d_v, 0, $d_z_m);
				    //中间数据段
				    $i++;
				    while ( $i < $d_z_s_n - 1 ) {
				        $tail_str = str_pad( $mallocs[$i+1], ZJRDB_HEAD_BLOCK_SIZE, ' ' );
				        $writeArr[$mallocs[$i]] = $tail_str . '00' . substr( $d_v, $d_z_m + ( $i - 1 ) * $d_z_s, $d_z_s );
				        $i++;
				    }
				    //结尾数据段
				    $tail_str = str_pad( 0, ZJRDB_HEAD_BLOCK_SIZE, ' ' );
				    $e_l = ( $d_l - $d_z_m ) % $d_z_s;  //剩余的长度
				    $d_p_m = $e_l / $d_z_s;
				    $data_percent = substr( ceil( $d_p_m * 100 ), -2, 2 ); //数据占用空间的百分比
				    $writeArr[$mallocs[$d_z_s_n-1]] = $tail_str . $data_percent . str_pad( substr( $d_v, - $e_l, $e_l ), $e_l, ' ' );
				}
				$j = 0;
				$rs = 0;
				foreach ( $writeArr as $index => $writeStr ) {
			        fseek( $this -> handle, $index );
			        $type = $j === 0 ? 1 : 2;
			        //如果开启了事务
			        if ( ! $this->trans_token ) {
				        if (fwrite($this->handle, $type . $writeStr, ZJRDB_BLOCK_SIZE)) {
				            if ( $j===0 ) {
				                $rs = $index;
				            }
				        }
			        } else {
			        	$this->recordTransData( $index, $type . $writeStr, ZJRDB_C );	//记录事物数据
			        }
			        //改变头部信息
			        if ( $this -> file_config['n_id'] < $index) {
			        	$this -> file_config['n_id'] = $index;
			        }
			        
				    $j++;
				}
				
				//没有启用事物时完成索引创建动作
				if ( ! $this->trans_token ) {
					//如果是关系型数据库，则要创建数据索引
					if ($this->file_config['type'] == 'RDS') {
						$this->createDataIndex($rs, $v[1]);
					}
					//插入成功开始创建索引
					elseif ( empty( $exist ) && ! empty( $rs ) ) {
						$this->createIndex(array($v[0] => $rs));
					}
					//改变头部
					$this -> headerChange();
				}
				$result[$v[0]] = $rs;
			}
		}
		return $result;
	}
	
	/**
	 * 释放空间
	 */
	public function freeSpace( $frees ) {
	    if ( ! empty( $frees ) ) {
	        $start = ftell( $this->handle );
	        foreach ( $frees as $free ) {
	            if ( empty( $free ) ) continue;
	            fseek( $this->handle, $free );    //移动到指定的位置
	            fwrite( $this -> handle, 0, 1 );
	            if ( $this -> file_config['inval_min'] > $free || ! $this -> file_config['inval_min'] ) {
	                $this -> file_config['inval_min'] = $free;
	            } elseif ( $this -> file_config['inval_max'] < $free ) {
	                $this -> file_config['inval_max'] = $free;
	            }
	        }
	        //将指针移回原来位置
	        fseek( $this->handle, $start );
	    }
	}
	
	/**
	 * 寻找空闲的空间
	 */
	private function seekFreeSpace( $nums ) {
	    if ( empty( $nums ) ) return array();
	    $seeks = array();
	    //通过头部给定的空间寻找
	    if ( $this -> file_config['inval_min'] ) {
	        $i = 0;
	        do {
	        	fseek($this->handle, $this -> file_config['inval_min'] );	//将文件指针移到指定位置
	            if ( fgetc( $this -> handle ) === '0' ) {
	                $seeks[$i] = $this -> file_config['inval_min'];
	                $i++;
	            }
	            //移动最小失效位
	            $this -> file_config['inval_min'] += ZJRDB_BLOCK_SIZE;
	            
	            if ( feof( $this -> handle ) || $this -> file_config['inval_min'] > $this -> file_config['inval_max'] ) {
	                $this -> file_config['inval_min'] = $this -> file_config['inval_max'] = 0;
	                break;
	            }
	        } while ( $nums > $i );
	        
	        //如果没有空闲的空间， 就开辟新空间
	        $new = $this->file_config['n_id'] + ZJRDB_BLOCK_SIZE; 
	        while ( $nums > $i ) {
	        	$seeks[$i] = $new;
	        	$new += ZJRDB_BLOCK_SIZE;
	        	$i++;
	        }
	    } else {
	        $seeks[0] = $this->file_config['n_id'] + ZJRDB_BLOCK_SIZE;
	        $i = 1;
	        do {
	            $seeks[$i] = $seeks[$i-1] + ZJRDB_BLOCK_SIZE;
	            $i++;
	        } while ( $nums > $i );
	    }
	    return $seeks;
	}
	
	/**
	 * 寻找原始数据空间
	 */
	private function seekSourceSpace() {
	    //将文件指针移到末尾
	    $seeks[0] = ftell( $this->handle );
	    $i = 1;
	    do {
	        fseek( $this->handle, 1, SEEK_CUR );
	        $tail = trim( fread( $this->handle, ZJRDB_HEAD_BLOCK_SIZE ) );
	        if ( $tail > 0 ) {
	            $seeks[$i] = $tail;
	            fseek( $this->handle, $tail );
	        }
	        $i++;
	    } while ( $tail > 0 );
	    //将指针移回原来位置
	    fseek( $this->handle, $seeks[0] );
	    return $seeks;
	}
	
	
	/**
	 * 设置数据过期
	 * @param unknown $data
	 */
	private function setInvalidKey($keyIndex) {
		if (!empty($keyIndex)) {
			if ($this->isset_transaction == false) {
				//修改文件失效时间
				foreach ($keyIndex as $key => $index) {
					if (!empty($index) && !empty($key)) {
						fseek($this->handle, $index+strlen($key)+1);
						fwrite($this->handle, 1, 1);
						if (fgetc($this->handle) != '|') {
							fwrite($this->handle, 1, 1);
						}
					}
				}
			} else {
				$this->transaction_data['delete'][] = $keyIndex;
			}
		}
	}

	private function initSearchData() {
		$this->seekData = array();
		$this->searchCount = 0;
		$this->dataCount = 0;
	}

	private function seekValByIndex($keys, $isInvalidData=false) {
		$data = array();
		$this->initSearchData();
		$indexDatas = $this->getIndex($keys);
		if (!empty($indexDatas)) {
			foreach ($indexDatas as $suffix => $indexData) {
				//切换表
				$this->changeTableSuffix($suffix, 'r');
				
				$this->seekDataByIndex($indexData, $isInvalidData);
			}
			$data = $this->seekData;
		}
		$this->initSearchData();
		return $data;
	}
	
	private function seekDataByIndex($indexData,$isInvalidData=false, $page=1, $prepage=-1) {
		$invalidData = array();
		$curTime = time();
		if (!empty($indexData)) {
			foreach ($indexData as $index => $nums) {
				if (!empty($index)) {
					fseek($this->handle, intval($index));
					$data_block = fread( $this->handle, ZJRDB_BLOCK_SIZE );	//取出数据块
					//判断是否有效数据
					$block_type = substr( $data_block, 0, 1 );
					$data_tail = trim( substr( $data_block, 1, ZJRDB_HEAD_BLOCK_SIZE ) );
					$data_key	= trim( substr( $data_block, 1+ZJRDB_HEAD_BLOCK_SIZE, ZJRDB_KEY_SIZE ) );
					$expire_time = trim( substr( $data_block, 1+ZJRDB_HEAD_BLOCK_SIZE+ZJRDB_KEY_SIZE, 10 ) );
					$data_str = substr( $data_block, 1+ZJRDB_HEAD_BLOCK_SIZE+ZJRDB_KEY_SIZE+12 );
					if ( $block_type == '1' && ( $expire_time === '0' || $expire_time > $curTime ) ) {
						//判断是否有辅助空间数据
						while ( $data_tail > 0 ) {
							fseek($this->handle, intval($data_tail) );
							$data_block = fread($this->handle, ZJRDB_BLOCK_SIZE );	//取出数据块
							//判断是否有效数据
							$block_type = substr( $data_block, 0, 1 );
							$data_tail = trim( substr( $data_block, 1, ZJRDB_HEAD_BLOCK_SIZE ) );
							if ( $block_type == '2' ) {
								$data_str .= substr( $data_block, ZJRDB_HEAD_BLOCK_SIZE+3 );
							}
						}
						$this->seekData[$data_key] = gzinflate( base64_decode( trim( $data_str ) ) );
					}
				}
			}
		}
	}
	
	private function seekValByKey($key = '') {
		$data = array();
		$curTime = time();
		$rowIndex = ZJRDB_BLOCK_SIZE;	//行指针
	    if ( $this->handle !== false ) {
	    	if (!empty($key)) {
	    		while (!feof($this->handle)) {
	    			//一块一块的寻找
	    			fseek( $this->handle, $rowIndex );
	    			$type = fgetc($this->handle);
	    			//判断数据是否被删除
	    			if ( $type == '1' ) {
	    				fseek( $this->handle, ZJRDB_HEAD_BLOCK_SIZE + ZJRDB_KEY_SIZE, SEEK_CUR );
	    				$expire_time = trim(fread( $this->handle, 10 ));
	    				//判断数据是否过期
	    				if ( $expire_time == '0' || $expire_time > $curTime ) {
	    					//判断数据是否满足查询条件
	    					fseek( $this->handle, $rowIndex + 1 + ZJRDB_HEAD_BLOCK_SIZE );
	    					$seekKey = trim( fread( $this->handle, ZJRDB_KEY_SIZE ) );
	    					if ($this->checkKeyIsSeek($key, $seekKey)) {
	    						//将指针移到 tail域
	    						fseek( $this->handle, $rowIndex + 1 );
	    						$data_tail = fread( $this->handle, ZJRDB_HEAD_BLOCK_SIZE );
	    						//将指针移到 data域
	    						fseek( $this->handle, ZJRDB_KEY_SIZE + 12, SEEK_CUR );
	    						$data_str = fread( $this->handle, ZJRDB_BLOCK_SIZE - 13 - ZJRDB_HEAD_BLOCK_SIZE - ZJRDB_KEY_SIZE );
	    						
	    						//判断是否有辅助空间数据
	    						while ( $data_tail > 0 ) {
	    							fseek($this->handle, intval( $data_tail ) );
	    							$data_block = fread($this->handle, ZJRDB_BLOCK_SIZE );	//取出数据块
	    							//判断是否有效数据
	    							$block_type = substr( $data_block, 0, 1 );
	    							$data_tail = trim( substr( $data_block, 1, ZJRDB_HEAD_BLOCK_SIZE ) );
	    							if ( $block_type == '2' ) {
	    								$data_str .= substr( $data_block, ZJRDB_HEAD_BLOCK_SIZE+3 );
	    							}
	    						}
	    						
	    						$data[$key][$seekKey] = gzinflate( base64_decode( trim( $data_str ) ) );
	    						
	    					}
	    				}
	    			}
	    			$rowIndex += ZJRDB_BLOCK_SIZE;
	    		}
	    	}
	    }
	    return $data;
	}
	
	private function rebuildConfig($str) {
		$str = trim($str);
		if (!empty($str) && empty($this->file_config)) {
			$this->file_config = json_decode($str, true);
		}
	}
	
	private function initConfigInfo() {
		if (!is_file($this->file_path)) return false;
		$reTimes = 0;
		do {
			$handle = fopen($this->file_path, 'r');
			fseek( $handle, 0 );
			$headStr = fgets($handle);
			$reTimes++;
		} while(empty($headStr) && $reTimes < 1000000);
		fclose($handle);
		$this->rebuildConfig($headStr);
	}
	
	private function checkKeyIsSeek ($key, $dbKey) {
		$pos = strpos($key, '%');
		if ($pos === false) {
			if ($key == $dbKey) return true;
		} else {
			$mode = '/^'.str_replace('%', '.*', $key).'$/i';
			if (preg_match($mode, $dbKey)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 创建索引
	 * zjr
	 */
	private function createIndex($data) {
		if (!empty($data)) {
			$path = $this->index_path;
			if (!is_dir($path)) mkdir($path);
			foreach ($data as $key => $index) {
				if (!empty($key) && !empty($index)) {
					$hashKey = md5($key);
					for ($i=0;$i<$this->index_type;$i++) {
						$path .= substr($hashKey, $i, 1).'/';
						if (!is_dir($path)) mkdir($path);
					}
					file_put_contents($path.$this->table.'.db', $key.'|'.$index.'|'.$this->table_suffix.'|0'.chr(10), FILE_APPEND);
				}
			}
		}
	}

	/**
	 * 获取索引
	 * zjr
	 */
	private function getIndex($keys, $isInvalidData=false) {
		$keys = array_unique($keys);
		$keyArr = array();
		$keyTable = array();
		if (!empty($keys)) {
			foreach ($keys as $key) {
				$path = $this->index_path;
				if (!empty($key)) {
					$hashKey = md5($key);
					for ( $i=0; $i<$this->index_type; $i++ ) {
						$path .= substr( $hashKey, $i, 1 ) . '/';
					}
					if (is_file($path.$this->table.'.db')) {
						$fp = fopen($path.$this->table.'.db', 'r+');
						flock($fp, LOCK_EX);
						while (!feof($fp)) {
							$str = fgets($fp);
							if (strpos($str, $key) === false) continue;
							$splitArr = explode('|', $str);
							if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
							$dbKey = $splitArr[0];
							$tableSufix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];
							if ($key == $dbKey) {
								$keyTable[$tableSufix][$key] = $splitArr[1];
							}
							if ($isInvalidData) {
								fseek($fp, -2, SEEK_CUR);
								fwrite($fp, 1, 1);
								fseek($fp, 1, SEEK_CUR);
							}
						}
						
						flock($fp, LOCK_UN);
						fclose($fp);
					}
				}
			}
		}
		if ( ! empty( $keyTable ) ) {
		    foreach ( $keyTable as $preffix => $arr ) {
		        if ( ! empty( $arr ) ) {
		            foreach ( $arr as $key => $index ) {
		                $keyArr[$preffix][$index] = 1;
		            }
		        }
		    }
		}
		return $keyArr;
	}

	/**
	* 删除文件夹
	* zjr
	*/
	private function deldir($dir) {
		$dirArr = glob($dir.'*');
		//先删除目录下的文件：
		if (!empty($dirArr)) {
			foreach ($dirArr as $path) {
				if(!is_dir($path)) {
					unlink($path);
				} else {
					$this->deldir($path.'/');
				}
			}
		}
		//删除当前文件夹：
		if(rmdir($dir)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	* 创建文件夹
	* zjr
	*/
	private function mkdirs($path,$i=0) {
		$path_out = preg_replace('/[^\/.]+\/?$/', '', $path);
		if (!is_dir($path_out)) {
			if($i<50){
				$this->mkdirs($path_out,++$i);
			}
		}
		mkdir($path);
	}

	
	/**
	 * 重新整理文件库，过滤无效数据
	 * zjr
	 */
	private function filterOverDateData() {
		if (!empty($this->file_config)) {
			if ($this->file_config['next_filter_time'] < time()) {
				file_put_contents($this->base_path, $this->file_path.chr(10), FILE_APPEND);
			}
		}
	}

	/**
	 * 重建索引
	 * zjr
	 */
	public function rebuildIndex() {

		if ($this->file_path) {
		    
			$handle = fopen($this->file_path, 'r');

			flock($handle, LOCK_SH);
			//清除索引
			$this->deldir($this->index_path);
			$rowIndex = 0;

			while (!feof($handle)) {
				$str = fgets($handle);
				if ($rowIndex != 0) {
					$splitPos = strpos($str, '|');
					$dbKey = substr($str, 0, $splitPos);
					$this->createIndex(array($dbKey => $rowIndex));
				}
				$rowIndex = ftell($handle);
			}
			
			flock($handle, LOCK_UN);

			fclose($handle);

			return true;
		}

	}


	/**
	 * 清理数据表： 清理表示需要对数据表进行写锁，阻止其他进程执行写操作
	 * zjr
	 */
	public function clearRubbish() {
		//给文件上独占锁
		if (empty($this->file_path)) return false;
		$this->handle = fopen($this->file_path, 'r');
		
		flock($this->handle, LOCK_EX);
		
		file_put_contents($this->file_path.'.bak', fgets($this->handle));
		//迁移数据
		$this->transferData($this->index_path);
		//备份主数据表
		copy($this->file_path, $this->file_path.date('YmdHis').'.bak');

		flock($this->handle, LOCK_UN);
		fclose($this->handle);

		//删除源文件
		if (unlink($this->file_path)) {
			//生成源文件
			if (rename($this->file_path.'.bak', $this->file_path)) {
				//重建索引
				$this->rebuildIndex();
			}
		}		
		unset($this->handle);
		return true;
	}

	private function transferData($indexPath) {
		$curTime = time();
		$dirArr = glob($indexPath.'*');
		if (!empty($dirArr)) {
			foreach ($dirArr as $path) {
				if(!is_dir($path)) {
					//取出所有索引数据
					$indexArr = array();
					$handle = fopen($path, 'r');
					while (!feof($handle)) {
						$tmpStr = fgets($handle);
						if (!empty($tmpStr)) {
							$tmpArr = explode('|', $tmpStr);
							if (!empty($tmpArr)) {
								$indexArr[$tmpArr[0]] = $tmpArr[1];
							}
						}
					}
					fclose($handle);
					unset($handle);
					//开始迁移数据
					if (!empty($indexArr)) {
						foreach ($indexArr as $key => $index) {
							if (!empty($key) && !empty($index)) {
								fseek($this->handle, intval($index));
								$tmpData = fgets($this->handle);
								$tmpDataArr = explode('|', $tmpData);
								if (!empty($tmpDataArr) && ($tmpDataArr[1] == 0 || $tmpDataArr[1] > $curTime)) {
									file_put_contents($this->file_path.'.bak', $tmpData, FILE_APPEND);
								}
							}
						}
					}
				} else {
					$this->transferData($path.'/');
				}
			}
		}
		return true;
	}

	/***************************************************
	 * 支持 关系型数据库
	 * zjr
	 ****************************************************/
	/**
	 * 查找
	 */
	public function select($filed, $where, $deep = '1,1', $page = 1, $prepage = 100, $sort = ''){
		//数据验证
		if (!is_array($where)) return false;
		if (empty($this->file_path) || $this->file_config['type'] != 'RDS') return false;
		$this->initSearchData();

		$this->handle = fopen($this->file_path, 'r');
		
		flock($this->handle, LOCK_SH);
		//首先获取所有的索引数据
		$dataIndexs = $this->get_data_index($where);
		if (trim($filed) == 'count(*)') {
			$count = 0;
			foreach ($dataIndexs as $suffix => $dataIndex) {
			    arsort($dataIndex);
				$count += count($dataIndex);
			}
			$data['count(*)'] = $count;
		} else {
			foreach ($dataIndexs as $suffix => $dataIndex) {
				if (empty($dataIndex)) continue;
				$this->changeTableSuffix($suffix, 'r');
				arsort($dataIndex);
				//获取根据索引获取数据
				$this->seekDataByIndex($dataIndex, false, $page, $prepage);
				if ($this->dataCount >= $prepage) break;
			}
			
			//对数据进行处理
			$data = $this->handle_datas($this->seekData, $filed, $deep);
		}
		
		flock($this->handle, LOCK_UN);
		fclose($this->handle);

		$this->initSearchData();
		unset($dataIndex);
		unset($this->handle);
		return $data;
	}
	
	/**
	 * 更新
	 */
	public function update($data, $where, $deep='1,1'){
		if (!is_array($where)) return false;
		if (empty($this->file_path) || $this->file_config['type'] != 'RDS') return false;
		$this->initSearchData();
		$retData = array();
		$this->handle = fopen($this->file_path, 'r+');
		//先读取索引数据
		$dataIndexs = $this->get_data_index($where, true);
		flock($this->handle, LOCK_EX);
		foreach ($dataIndexs as $suffix => $dataIndex) {
			if (empty($dataIndex)) continue;
			//根据索引获取数据	并且打开数据过期开关
			$this->changeTableSuffix($suffix, 'r+');
			$this->seekDataByIndex($dataIndex, true);
		}
		//重组数据后重新生成
		$retData = $this->rebuild_update_datas($this->seekData, $data, $deep);

		flock($this->handle, LOCK_UN);
		fclose($this->handle);

		$this->initSearchData();
		unset($dataIndex);
		unset($this->handle);
		return $retData;
	}
	
	/**
	 * 插入
	 */
	public function insert($data){
		if (empty($this->file_path) || $this->file_config['type'] != 'RDS') return false;
		$this->handle = fopen($this->file_path, 'r+');
		
		flock($this->handle, LOCK_EX);
		
		$res = $this->insert_rds($data);
		
		flock($this->handle, LOCK_UN);
		fclose($this->handle);

		unset($this->handle);
		return $res;
	}
	
	/**
	 * 删除
	 */
	public function delete($where, $deep='1,1'){
		if (!is_array($where)) return false;
		if (empty($this->file_path) || $this->file_config['type'] != 'RDS') return false;
		$this->initSearchData();
		//先读取索引数据
		$dataIndexs = $this->get_data_index($where, true);
		//失效指向该数据的所有索引数据

		$this->handle = fopen($this->file_path, 'r+');
		$sourceData = array();
		foreach ($dataIndexs as $suffix => $dataIndex) {
			if (empty($dataIndex)) continue;
			//根据索引获取数据	并且打开数据过期开关
			$this->changeTableSuffix($suffix, 'r+');
			$this->seekDataByIndex($dataIndex, true);
		}
		flock($this->handle, LOCK_UN);
		fclose($this->handle);
		
		$this->initSearchData();
		unset($dataIndex);
		unset($sourceData);
		unset($this->handle);
		return true;
	}

	private function insert_rds($data) {
		if (!empty($data)) {
			$key = (microtime(true) *10000) . mt_rand(10, 99);
			$this->insert_rds_core( $data );
			return $key;
		}
		return false;
	}
	
	private function insert_rds_core( $data ) {
	    $nowTime = time();
	    $result = array();
	    if ( ! empty( $data ) ) {
	        //最新ID
	        $n_id = ++$this->file_config['n_id'];
	        
	        //先对数据进行压缩， 判断数据百分比 开始分配数据空间
	        $d_v = base64_encode( gzdeflate( serialize( $data ) ) );   //压缩后的数据
	        $d_l = strlen( $d_v );  //数据字节数
	        $d_z_m = ZJRDB_RDS_BLOCK_SIZE - ZJRDB_HEAD_BLOCK_SIZE - ZJRDB_DATATYPE_SIZE;  //主数据空间
	        
	        $d_z_s_n = ceil( $d_l / $d_z_m );  //计算需要的空间块
	        
	        
	        foreach ($keyValArr as $v) {
	            $mallocs = array();
	            //查找数据是否存在
	            $exist = $this->getIndex(array($v[0]));
	            if ( !empty( $exist ) ) {
	                //定位表
	                $this->changeTableSuffix( key($exist), 'r+');
	                //将文件指针移到末尾
	                fseek($this->handle, key(end($exist)));
	                //获取原始数据空间
	                $mallocs = $this -> seekSourceSpace();
	            } else {
	                //重置表
	                $this->setLatestTable();
	                 
	                //将文件指针移到末尾
	                fseek($this->handle, $this->file_config['n_id'] + ZJRDB_BLOCK_SIZE );
	            }
	            if (empty($v) || empty($v[0])) continue;
	            $invalidTime = empty($v[2]) ? 0 : ($nowTime + intval($v[2]));
	
	            //先对数据进行压缩， 判断数据百分比 开始分配数据空间
	            $d_z_m = ZJRDB_BLOCK_SIZE - ZJRDB_KEY_SIZE - ZJRDB_HEAD_BLOCK_SIZE - 13;  //主数据空间
	            $d_z_s = $d_z_m + 10 + ZJRDB_KEY_SIZE;        //辅助数据空间
	            $d_v = base64_encode( gzdeflate( $v[1] ) );     //压缩后的数据
	            $d_l = strlen( $d_v );  //数据字节数
	            $d_p_m = $d_l / $d_z_m ;  //主数据空间占用百分比
	
	            //计算需要的空间块
	            $d_z_s_n = 1;
	            if ( $d_p_m > 1 ) {
	                $d_z_s_n += ceil( ( $d_l - $d_z_m ) / $d_z_s );
	            }
	            //计算需要再分配的空间数
	            $source_space_count = count( $mallocs );
	            if ( $source_space_count < $d_z_s_n ) {
	                //新增分配空间
	                $rellocs = $this -> seekFreeSpace( $d_z_s_n - $source_space_count );
	                $mallocs = array_merge( $mallocs, $rellocs );
	            }
	            //如果需要的空间大于分配的空间，则需要释放多余的空间
	            elseif ( $source_space_count > $d_z_s_n ) {
	                $free_space = array_slice( $mallocs, $d_z_s_n );
	                //释放空间
	                $this -> freeSpace( $free_space );
	            }
	            $writeArr = array();
	            //对只有主数据空间的数据块
	            if ( $d_z_s_n == 1 ) {
	                $tail_str = str_pad( 0, ZJRDB_HEAD_BLOCK_SIZE, ' ' );
	                $key_str = str_pad( $v[0], ZJRDB_KEY_SIZE, ' ' );
	                $expire_time_str = str_pad( $invalidTime, 10, ' ' );
	                $data_percent = str_pad( substr( ceil( $d_p_m * 100 ), -2, 2 ), 2, '0', STR_PAD_LEFT ); //数据占用空间的百分比
	                $writeArr[$mallocs[0]] = $tail_str . $key_str . $expire_time_str . $data_percent . str_pad( $d_v, $d_z_m, ' ' );
	            } else {
	                //主数据段
	                $i = 0;
	                $tail_str = str_pad( $mallocs[$i+1], ZJRDB_HEAD_BLOCK_SIZE, ' ' );
	                $key_str = str_pad( $v[0], ZJRDB_KEY_SIZE, ' ' );
	                $expire_time_str = str_pad( $invalidTime, 10, ' ' );
	                $data_percent = substr( ceil( $d_p_m * 100 ), -2, 2 ); //数据占用空间的百分比
	                $writeArr[$mallocs[$i]] = $tail_str . $key_str . $expire_time_str . '00' . substr($d_v, 0, $d_z_m);
	                //中间数据段
	                $i++;
	                while ( $i < $d_z_s_n - 1 ) {
	                    $tail_str = str_pad( $mallocs[$i+1], ZJRDB_HEAD_BLOCK_SIZE, ' ' );
	                    $writeArr[$mallocs[$i]] = $tail_str . '00' . substr( $d_v, $d_z_m + ( $i - 1 ) * $d_z_s, $d_z_s );
	                    $i++;
	                }
	                //结尾数据段
	                $tail_str = str_pad( 0, ZJRDB_HEAD_BLOCK_SIZE, ' ' );
	                $e_l = ( $d_l - $d_z_m ) % $d_z_s;  //剩余的长度
	                $d_p_m = $e_l / $d_z_s;
	                $data_percent = substr( ceil( $d_p_m * 100 ), -2, 2 ); //数据占用空间的百分比
	                $writeArr[$mallocs[$d_z_s_n-1]] = $tail_str . $data_percent . str_pad( substr( $d_v, - $e_l, $e_l ), $e_l, ' ' );
	            }
	            $j = 0;
	            $rs = 0;
	            foreach ( $writeArr as $index => $writeStr ) {
	                fseek( $this -> handle, $index );
	                $type = $j === 0 ? 1 : 2;
	                //如果开启了事务
	                if ( ! $this->trans_token ) {
	                    if (fwrite($this->handle, $type . $writeStr, ZJRDB_BLOCK_SIZE)) {
	                        if ( $j===0 ) {
	                            $rs = $index;
	                        }
	                    }
	                } else {
	                    $this->recordTransData( $index, $type . $writeStr, ZJRDB_C );	//记录事物数据
	                }
	                //改变头部信息
	                if ( $this -> file_config['n_id'] < $index) {
	                    $this -> file_config['n_id'] = $index;
	                }
	                 
	                $j++;
	            }
	
	            //没有启用事物时完成索引创建动作
	            if ( ! $this->trans_token ) {
	                //如果是关系型数据库，则要创建数据索引
	                if ($this->file_config['type'] == 'RDS') {
	                    $this->createDataIndex($rs, $v[1]);
	                }
	                //插入成功开始创建索引
	                elseif ( empty( $exist ) && ! empty( $rs ) ) {
	                    $this->createIndex(array($v[0] => $rs));
	                }
	                //改变头部
	                $this -> headerChange();
	            }
	            $result[$v[0]] = $rs;
	        }
	    }
	    return $result;
	}


	/**
	 * 构建数据索引树
	 * zjr
	 */
	private function createDataIndex($key, $data, $name='') {
		$data = is_array($data) ? $data : json_decode($data, true);
		if (!empty($data)) {
			if (is_array($data)) {
				foreach ($data as $nameKey => $value) {
					if (!is_numeric($nameKey)) $name= $nameKey;
					//为name 键文件夹
					if (!empty($value)) {
						if (is_array($value)) {
							$this->createDataIndex($key, $value, $name);
						} elseif (is_numeric($value)) {
							$this->createNumberIndex($key, $value, $name);
						} else {
							$this->createStringIndex($key, $value, $name);
						}
					}
				}
			} elseif (is_numeric($data)) {
				$this->createNumberIndex($key, $data, '_common');
			} else {
				$this->createStringIndex($key, $data, '_common');
			}
		}
	}

	/**
	 * 创建字符串索引
	 * zjr
	 */
	private function createStringIndex($dataKey, $val, $name='') {
		//为值创建索引目录
		$hashVal = md5($val);
		$path = $this->data_index_path;
		for ($i=0;$i<$this->index_type;$i++) {
			$path .= substr($hashVal, $i, 1).'/';
			if (!is_dir($path)) mkdir($path);
		}
		file_put_contents($path.$name.'.db', $hashVal.'|'.$dataKey.'|'.$this->table_suffix.'|0'.chr(10), FILE_APPEND);
	}

	/**
	 * 创建数字索引
	 * zjr
	 */
	private function createNumberIndex($dataKey, $val, $name='') {
		//为值创建索引目录
		$path = $this->number_index_path;
		// $hashVal = md5($val);
		$intPart = strpos($val, '.') !== false ? substr($val, 0, strpos($val, '.')) : $val;
		$len = strlen($intPart);
		$path .= $len.'/';
		if (!is_dir($path)) mkdir($path);
		$indexType = $this->index_type - 1;
		$indexType = $len > $indexType ? $indexType : $len;
		if ($indexType > 0) {
			for ($i=0; $i<$indexType; $i++) {
				$path .= substr($intPart, $i, 1).'/';
				if (!is_dir($path)) mkdir($path);
			}
		}
		file_put_contents($path.$name.'.db', $val.'|'.$dataKey.'|'.$this->table_suffix.'|0'.chr(10), FILE_APPEND);
	}	
	
	private function get_data_index($where, $isInvalidData=false, $sort='') {
		$cacheKey = md5(json_encode($where));
		if (isset($this->cacheIndexData[$cacheKey]) && !empty($this->cacheIndexData[$cacheKey])) {
			return $this->cacheIndexData[$cacheKey];
		}
		$resultKeyArr = array();
		if (!empty($where)) {
			$this->searchKeyArr = array();
			$this->condition = array();

			//如果查询条件有多个的话，使用多线程， 否则使用单线程
			$threadSwitch = count($where) > 1 ? $this->isOpenThread : false;
			//使用
			if ($threadSwitch) {
				$searchKeyArr = new PStorage();
				$runArr = array();
			}
			foreach ($where as $nameStr => $val) {
				if ($val == '*') {
					$this->foreachDataIndex($nameStr, $val, $this->data_index_path, $isInvalidData);
				} else {

					if ($threadSwitch) {
						$tmpObj = new GetIndex($searchKeyArr, $this->data_index_path, $this->number_index_path);
						$tmpObj->setParams(array($nameStr, $val, $isInvalidData));
						$tmpObj->start();
						$runArr[] = $tmpObj;
					} else {
						$this->getKeyArr($nameStr, $val, $isInvalidData);
					}
				}
			}

			if ($threadSwitch) {
				foreach ($runArr as $runObj) {
					$runObj->join();
				}
				if (!empty($searchKeyArr)) {
					foreach ($searchKeyArr as $keyStr) {
						$splitArr = explode('|', $keyStr);
						if ( ! isset( $this->searchKeyArr[$splitArr[0]][$splitArr[1]][$splitArr[2]][$splitArr[3]] ) ) {
						    $this->searchKeyArr[$splitArr[0]][$splitArr[1]][$splitArr[2]][$splitArr[3]] = 1;
						} else {
						    $this->searchKeyArr[$splitArr[0]][$splitArr[1]][$splitArr[2]][$splitArr[3]]++;
						}
						$this->condition[$splitArr[2]][$splitArr[3]] = $splitArr[3];
					}
				}
				unset($searchKeyArr);
			}
		}
		if ( ! empty( $sort ) ) {
		}
		
		if (!empty($this->searchKeyArr)) {
			foreach ($this->searchKeyArr as $suffix => $datas) {
				foreach ($datas as $dbKey => $names) {
					if ((isset($names['or']) && count($names['or']) > 0) || (isset($names['and']) && count($names['and']) == count($this->condition['and']))) {
					    $keys = current( $names );
					    $resultKeyArr[$suffix][$dbKey] = 0;
					    if ( ! empty( $keys ) ) {
					        foreach ( $keys as $key => $nums ) {
					            $resultKeyArr[$suffix][$dbKey] += $nums;
					        }
					    }
						$resultKeyArr[$suffix][$dbKey] = current( current( $names ) );
					}
					if (isset($names['not']) && isset($resultKeyArr[$suffix][$dbKey])) {
						unset($resultKeyArr[$suffix][$dbKey]);
					}
				}
			}
		}
		$this->searchKeyArr = array(); // 清空临时数组
		$this->condition = array();
		if (memory_get_usage(true) > 31457280) $this->cacheIndexData = null;	//设置了最大缓存30M时会清空缓存
		if (!empty($resultKeyArr)) {
			$this->cacheIndexData[$cacheKey] = $resultKeyArr;
		}
		return $resultKeyArr;
	}
	
	private function getKeyArr($nameStr, $val, $isInvalidData=false) {
		if (!empty($nameStr)) {
			$nameArr = explode('@', $nameStr);
			$condition = isset($nameArr[1]) ? array_flip(explode(':', $nameArr[1])) : array();
			if (!empty($condition) && isset($condition['n'])) {
				$this->getNumberIndex($nameStr, $val, $isInvalidData);
			} else {
				$this->getStringIndex($nameStr, $val, $isInvalidData);
			}
		}
	}

	private function getStringIndex($nameStr, $valArr, $isInvalidData) {
		$nameArr = explode('@', $nameStr);
		$name = trim($nameArr[0]);
		$type = !empty($nameArr[1]) ? strtolower($nameArr[1]) : 'and';
		if (is_numeric($name)) return;
		$this->condition[$type][$name] = $name;
		$valArr = is_array($valArr) ? $valArr : explode(',', $valArr);
		foreach ($valArr as $val) {
			$hashVal = md5(trim($val));
			$path = $this->data_index_path;
			for ($i=0;$i<$this->index_type;$i++) {
				$path .= substr($hashVal, $i, 1).'/';
			}
			if (is_file($path.$name.'.db')) {
			
				$fp = fopen($path.$name.'.db', 'r+');
				flock($fp, LOCK_EX);
				while (!feof($fp)) {
					$str = fgets($fp);
					if (strpos($str, $hashVal) === false) continue;
					$splitArr = explode('|', $str);
					if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
					$dataIndexKey = $splitArr[0];
					$dbKey = $splitArr[1];
					$tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];
					if ($hashVal == $dataIndexKey) {
						if ( ! isset( $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name] ) ) {
						   $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name] = 1;
						} else {
						    $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name] ++;
						}
					}
					if ($isInvalidData) {
						fseek($fp, -2, SEEK_CUR);
						fwrite($fp, 1, 1);
						fseek($fp, 1, SEEK_CUR);
					}
				}
			
				flock($fp, LOCK_UN);
				fclose($fp);
			}
		}
	}

	private function parseNameStr($nameStr, $val) {
		$arr = array();
		$val = is_array($val) ? $val : explode(',', str_replace(' ', '', $val));
		$nameArr = explode('@', $nameStr);
	    $arr['name'] = trim($nameArr[0]);
	    $type = !empty($nameArr[1]) ? strtolower($nameArr[1]) : '';
	    $typeArr = empty($type) ? array('and' => 0) : array_flip(explode(':', $type));

	    if (empty($val)) return $arr;
		krsort($val);

    	if (isset($typeArr['or'])) {
    		$arr['condition'] = 'or';
    	} else {
    		$arr['condition'] = 'and';
    	}

    	if (isset($typeArr['>='])) {
    		$arr['type']['>='] = array_pop($val);
    	} elseif (isset($typeArr['>'])) {
			$arr['type']['>'] = array_pop($val);
    	}

    	if (empty($val)) return $arr;
    	if (isset($typeArr['<='])) {
    		$arr['type']['<='] = array_pop($val);
    	} elseif (isset($typeArr['<'])) {
			$arr['type']['<'] = array_pop($val);
    	}

    	if (isset($typeArr['!=']) && !empty($val)) {
    		$arr['type']['!='] = array_pop($val);
    	}

	    return $arr;
	}

	private function getIntPart($number) {
		return strpos($number, '.') !== false ? substr($number, 0, strpos($number, '.')) : $number;
	}

	private function getNumberIndex($nameStr, $val, $isInvalidData) {
	    $condition = $this->parseNameStr($nameStr, $val);
	    if (empty($condition)) return;
	    $this->condition[$condition['condition']][$condition['name']] = $condition['name'];
	    $path = $this->number_index_path;

	    if (isset($condition['type'])) {
	    	$numberIndexs = glob($path.'*');
	    	if (!empty($numberIndexs)) {
	    		$con = array();
	    		if (isset($condition['type']['>']) || isset($condition['type']['>='])) {
    				//获取整数部分
    				$number = isset($condition['type']['>']) ? $condition['type']['>'] : $condition['type']['>='];
    				$intPart = $this->getIntPart($number);
    				$len = strlen($intPart);
    				$con[] = '>='.$len;
    			}
    			if (isset($condition['type']['<']) || isset($condition['type']['<='])) {
    				//获取整数部分
    				$number = isset($condition['type']['<']) ? $condition['type']['<'] : $condition['type']['<='];
    				$intPart = $this->getIntPart($number);
    				$len = strlen($intPart);
    				$con[] = '<='.$len;
    			}
    			if (!empty($con)) {
    				foreach ($numberIndexs as $bytePath) {
		    			$byte = basename($bytePath);
	    				//位数验证
	    				$flag = true;
	    				foreach ($con as $conValue) {
	    					$compare = $byte.$conValue;
		    				eval("\$compareRes=$compare;");
			    			if (!$compareRes) $flag = false;
	    				}
		    			//通过位数验证后向下取位
		    			if ($flag) {
		    				$this->foreachNumberIndex($bytePath.'/', $condition, 0, $isInvalidData);
		    			}
		    		}
    			}

	    	}
	    	
	    } else {
	    	if (!is_array($val)) {
	    		$val = explode(',', $val);
	    	}
	    	if (!empty($val)) {
	    		foreach ($val as $number) {
	    			$intPart = $this->getIntPart($number);
					$len = strlen($intPart);
					$curPath = $path.$len.'/';
					$indexType = $this->index_type - 1;
					$indexType = $len > $indexType ? $indexType : $len;
					if ($indexType > 0) {
						for ($i=0; $i<$indexType; $i++) {
							$curPath .= substr($intPart, $i, 1).'/';
						}
					}
					if (is_file($curPath.$condition['name'].'.db')) {

				        if ($isInvalidData) {
							$fp = fopen($curPath.$condition['name'].'.db', 'r+');
							flock($fp, LOCK_EX);
						} else {
							$fp = fopen($curPath.$condition['name'].'.db', 'r');
							flock($fp, LOCK_SH);
						}

				        while (!feof($fp)) {
				            $str = fgets($fp);
				            if (strpos($str, $number) === false) continue;
				            $splitArr = explode('|', $str);
				            if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
				            $dataIndexKey = $splitArr[0];
				            $dbKey = $splitArr[1];
				            $tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];
				            if ($number == $dataIndexKey) {
				                if ( ! isset( $this->searchKeyArr[$tableSuffix][$dbKey][$condition['condition']][$condition['name']] ) )
				                    $this->searchKeyArr[$tableSuffix][$dbKey][$condition['condition']][$condition['name']] = 1;
				                else 
				                    $this->searchKeyArr[$tableSuffix][$dbKey][$condition['condition']][$condition['name']]++;
				            }
				            if ($isInvalidData) {
				                fseek($fp, -2, SEEK_CUR);
				                fwrite($fp, 1, 1);
				                fseek($fp, 1, SEEK_CUR);
				            }
				        }
				    
				        flock($fp, LOCK_UN);
				        fclose($fp);
				    }
	    		}
	    	}
	    }
	}

	private function foreachNumberIndex($dirPath, $condition, $dirIndex=0, $isInvalidData=false) {
		if (!empty($condition)) {
			$paths = glob($dirPath.'*');
			if (!empty($paths)) {
				foreach ($paths as $path) {
					$curName = basename($path, '.db');
					if (is_dir($path)) {
						//验证首位
						$con = array();
			    		if (isset($condition['type']['>']) || isset($condition['type']['>='])) {
		    				//获取首位数
		    				$number = isset($condition['type']['>']) ? $condition['type']['>'] : $condition['type']['>='];
		    				$byte = substr($number, $dirIndex, 1);
		    				$con[] = '>='.(empty($byte) ? 0 : $byte);
		    			}
		    			if (isset($condition['type']['<']) || isset($condition['type']['<='])) {
		    				//获取首位数
		    				$number = isset($condition['type']['<']) ? $condition['type']['<'] : $condition['type']['<='];
		    				$byte = substr($number, $dirIndex, 1);
		    				$con[] = '<='.(empty($byte) ? 0 : $byte);
		    			}
		    			$flag = true;
		    			if (!empty($con)) {
		    				//首位验证
		    				foreach ($con as $conValue) {
		    					if (strlen($curName) < $dirIndex+1) {
		    						$tmpName = 0;
		    					} else {
									$tmpName = substr($curName, $dirIndex, 1);
		    					}
		    					$compare = $tmpName.$conValue;
			    				eval("\$compareRes=$compare;");
				    			if (!$compareRes) $flag = false;
		    				}
		    			}
		    			if ($flag) $this->foreachNumberIndex($path.'/', $condition, ++$dirIndex,$isInvalidData);

					} else {
						$name = $condition['name'];
						if ($curName != $name) continue;
						//组装条件
						$con = array();
						if (!empty($condition['type'])) {
							foreach ($condition['type'] as $key => $value) {
								$con[] = $key.$value;
							}
						}
						$type = $condition['condition'];

						if ($isInvalidData) {
							$fp = fopen($path, 'r+');
							flock($fp, LOCK_EX);
						} else {
							$fp = fopen($path, 'r');
							flock($fp, LOCK_SH);
						}

						while (!feof($fp)) {
							$str = fgets($fp);
							if (empty($str)) continue;
							$splitArr = explode('|', $str);
							if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
							$dataIndexKey = $splitArr[0];
							$dbKey = $splitArr[1];
							$tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];

							$flag = true;
							//验证条件
							if (!empty($con)) {
								foreach ($con as $conValue) {
									$compare = $dataIndexKey.$conValue;
				    				eval("\$compareRes=$compare;");
					    			if (!$compareRes) $flag = false;
								}
							}
							if ($flag) {
							    if ( ! isset( $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name] ) ) {
							        $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name] = 1;
							    } else {
							        $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name]++;
							    }
							}

							if ($isInvalidData) {
								fseek($fp, -2, SEEK_CUR);
								fwrite($fp, 1, 1);
								fseek($fp, 1, SEEK_CUR);
							}
						}
					
						flock($fp, LOCK_UN);
						fclose($fp);
					}
				}
			} 
		}
	}

	private function foreachDataIndex($nameStr, $val, $dirPath, $isInvalidData=false) {
		if (!empty($val)) {
			$paths = glob($dirPath.'*');
			if (!empty($paths)) {
				$nameArr = explode('@', $nameStr);
				$name = $nameArr[0];
				foreach ($paths as $path) {
					if (is_dir($path)) {
						$this->foreachDataIndex($nameStr, $val, $path.'/', $isInvalidData);
					} else {
						$type = 'or';

						if ($isInvalidData) {
							$fp = fopen($path, 'r+');
							flock($fp, LOCK_EX);
						} else {
							$fp = fopen($path, 'r');
							flock($fp, LOCK_SH);
						}

						while (!feof($fp)) {
							$str = fgets($fp);
							if (empty($str)) continue;
							$splitArr = explode('|', $str);
							if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
							$dataIndexKey = $splitArr[0];
							$dbKey = $splitArr[1];
							$tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];
							if ($val == '*') {
							    if (!isset($this->searchKeyArr[$tableSuffix][$dbKey])) {
							        $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name] = 1;
							    } else {
							        $this->searchKeyArr[$tableSuffix][$dbKey][$type][$name]++;
							    }
							}
							if ($isInvalidData) {
								fseek($fp, -2, SEEK_CUR);
								fwrite($fp, 1, 1);
								fseek($fp, 1, SEEK_CUR);
							}
						}
					
						flock($fp, LOCK_UN);
						fclose($fp);
					}
				}
			} 
		}
	}
	
	/**
	 * 处理查询数据
	 */
	private function handle_datas($data, $filed, $deep) {
		$retData = array();
		$filed = empty($filed) ? '*' : (is_array($filed) ? implode(',', $filed) : $filed);
		$deepArr = !empty($deep) ? explode(',', $deep) : array(1,1);
		$minDeep = !empty($deepArr[0]) ? $deepArr[0] : 1;
		$maxDeep = !empty($deepArr[1]) ? $deepArr[1] : 0;
		if (!empty($data)) {
			foreach ($data as $index => $val) {
				if ($filed == '*') {
					$retData['#'.$index] = json_decode($val, true);
				} else {
					$this->searchTree(json_decode($val, true), $filed, 1, $minDeep, $maxDeep);
					if (!empty($this->searchTreeData)) {
						$retData['#'.$index] = $this->searchTreeData;
						$this->searchTreeData = null;
					}
				}
			}
		}
		return $retData;
	}
	
	/**
	 * 遍历树, 寻找数据
	 */
	private function searchTree($data, $filed, $curDeep, $minDeep, $maxDeep=0) {
		if (!empty($data) && ($curDeep <= $maxDeep || $maxDeep == 0)) {
			foreach ($data as $name => $val) {
				if (is_numeric($name) || empty($val)) continue;
				if (strpos($filed, $name) !== false && $curDeep >= $minDeep) {
					$this->searchTreeData[$name] = $val;
				}
				if (is_array($val)) {
					$this->searchTree($val, $filed, ++$curDeep, $minDeep, $maxDeep);
				}
			}
		}
	}

	/**
	 * 重组修改数据
	 */
	private function rebuild_update_datas($data, $updateDatas, $deep) {
		$retData = array();
		$deepArr = !empty($deep) ? explode(',', $deep) : array(1,1);
		$minDeep = !empty($deepArr[0]) ? $deepArr[0] : 1;
		$maxDeep = !empty($deepArr[1]) ? $deepArr[1] : 0;
		if (!empty($data)) {
			foreach ($data as $index => $val) {
				$sourceData = json_decode($val, true);
				$this->rebuildTree($sourceData, $updateDatas, 1, $minDeep, $maxDeep);
				if (!empty($sourceData)) {
					$res = $this->insert_core(array(array($index, json_encode($sourceData), 0)));
					$retData[$index] = $res[$index];
				}
			}
		}
		return $retData;
	}
	
	/**
	 * 遍历树, 重组修改数据
	 */
	private function rebuildTree(&$sourceDatas, &$updateDatas, $curDeep, $minDeep, $maxDeep=0) {
		if (!empty($sourceDatas) && ($curDeep <= $maxDeep || $maxDeep == 0)) {
			foreach ($sourceDatas as $name => &$val) {
				if (is_numeric($name) || empty($val)) continue;
				if (isset($updateDatas[$name]) && $curDeep >= $minDeep) {
					$val = $updateDatas[$name];
				} elseif (is_array($val)) {
					$this->rebuildTree($val, $updateDatas, ++$curDeep, $minDeep, $maxDeep);
				}
			}
		}
	}
	
}

if (ZJRDB_IS_OPEN_THREAD == true) {
class GetIndex extends Thread {
	private $index_type = 4;
	private $data_index_path = '';
	private $number_index_path = '';
	private $params = array();
    
	public function __construct($storage, $dataIndexPath, $numberIndexPath) {
	    $this->searchKeyArr = $storage;
	    $this->data_index_path = $dataIndexPath;
	    $this->number_index_path = $numberIndexPath;
	}
	public function run() {
		if (!empty($this->params)) {
			$this->getKeyArr($this->params[0], $this->params[1], $this->params[2]);
		}
	}

	public function setParams($params) {
		if(!empty($params)) {
			$this->params = $params;
		}
	}

	private function getKeyArr($nameStr, $val, $isInvalidData=false) {
		if (!empty($nameStr)) {
			$nameArr = explode('@', $nameStr);
			$condition = isset($nameArr[1]) ? array_flip(explode(':', $nameArr[1])) : array();
			if (!empty($condition) && isset($condition['n'])) {
				$this->getNumberIndex($nameStr, $val, $isInvalidData);
			} else {
				$this->getStringIndex($nameStr, $val, $isInvalidData);
			}
		}
	}

	private function getStringIndex($nameStr, $valArr, $isInvalidData) {
		$nameArr = explode('@', $nameStr);
		$name = trim($nameArr[0]);
		$type = !empty($nameArr[1]) ? strtolower($nameArr[1]) : 'and';
		if (is_numeric($name)) return;
		$valArr = is_array($valArr) ? $valArr : explode(',', $valArr);
		foreach ($valArr as $val) {
			$hashVal = md5(trim($val));
			$path = $this->data_index_path;
			for ($i=0;$i<$this->index_type;$i++) {
				$path .= substr($hashVal, $i, 1).'/';
			}
			if (is_file($path.$name.'.db')) {
			
				$fp = fopen($path.$name.'.db', 'r+');
				flock($fp, LOCK_EX);
				while (!feof($fp)) {
					$str = fgets($fp);
					if (strpos($str, $hashVal) === false) continue;
					$splitArr = explode('|', $str);
					if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
					$dataIndexKey = $splitArr[0];
					$dbKey = $splitArr[1];
					$tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];
					if ($hashVal == $dataIndexKey) {
						$this->searchKeyArr[] = "$tableSuffix|$dbKey|{$type}|{$name}";
					}
					if ($isInvalidData) {
						fseek($fp, -2, SEEK_CUR);
						fwrite($fp, 1, 1);
						fseek($fp, 1, SEEK_CUR);
					}
				}
			
				flock($fp, LOCK_UN);
				fclose($fp);
			}
		}
	}

	private function parseNameStr($nameStr, $val) {
		$arr = array();
		$val = is_array($val) ? $val : explode(',', str_replace(' ', '', $val));
		$nameArr = explode('@', $nameStr);
	    $arr['name'] = trim($nameArr[0]);
	    $type = !empty($nameArr[1]) ? strtolower($nameArr[1]) : '';
	    $typeArr = empty($type) ? array('and' => 0) : array_flip(explode(':', $type));

	    if (empty($val)) return $arr;
		krsort($val);

    	if (isset($typeArr['or'])) {
    		$arr['condition'] = 'or';
    	} else {
    		$arr['condition'] = 'and';
    	}

    	if (isset($typeArr['>='])) {
    		$arr['type']['>='] = array_pop($val);
    	} elseif (isset($typeArr['>'])) {
			$arr['type']['>'] = array_pop($val);
    	}

    	if (empty($val)) return $arr;
    	if (isset($typeArr['<='])) {
    		$arr['type']['<='] = array_pop($val);
    	} elseif (isset($typeArr['<'])) {
			$arr['type']['<'] = array_pop($val);
    	}

    	if (isset($typeArr['!=']) && !empty($val)) {
    		$arr['type']['!='] = array_pop($val);
    	}

	    return $arr;
	}

	private function getIntPart($number) {
		return strpos($number, '.') !== false ? substr($number, 0, strpos($number, '.')) : $number;
	}

	private function getNumberIndex($nameStr, $val, $isInvalidData) {
	    $condition = $this->parseNameStr($nameStr, $val);
	    if (empty($condition)) return;
	    $path = $this->number_index_path;
	    if (isset($condition['type'])) {
	    	$numberIndexs = glob($path.'*');
	    	if (!empty($numberIndexs)) {
	    		$con = array();
	    		if (isset($condition['type']['>']) || isset($condition['type']['>='])) {
    				//获取整数部分
    				$number = isset($condition['type']['>']) ? $condition['type']['>'] : $condition['type']['>='];
    				$intPart = $this->getIntPart($number);
    				$len = strlen($intPart);
    				$con[] = '>='.$len;
    			}
    			if (isset($condition['type']['<']) || isset($condition['type']['<='])) {
    				//获取整数部分
    				$number = isset($condition['type']['<']) ? $condition['type']['<'] : $condition['type']['<='];
    				$intPart = $this->getIntPart($number);
    				$len = strlen($intPart);
    				$con[] = '<='.$len;
    			}
    			if (!empty($con)) {
    				foreach ($numberIndexs as $bytePath) {
		    			$byte = basename($bytePath);
	    				//位数验证
	    				$flag = true;
	    				foreach ($con as $conValue) {
	    					$compare = $byte.$conValue;
		    				eval("\$compareRes=$compare;");
			    			if (!$compareRes) $flag = false;
	    				}
		    			//通过位数验证后向下取位
		    			if ($flag) {
		    				$this->foreachNumberIndex($bytePath.'/', $condition, 0, $isInvalidData);
		    			}
		    		}
    			}

	    	}
	    	
	    } else {
	    	if (!is_array($val)) {
	    		$val = explode(',', $val);
	    	}
	    	if (!empty($val)) {
	    		foreach ($val as $number) {
	    			$intPart = $this->getIntPart($number);
					$len = strlen($intPart);
					$curPath = $path.$len.'/';
					$indexType = $this->index_type - 1;
					$indexType = $len > $indexType ? $indexType : $len;
					if ($indexType > 0) {
						for ($i=0; $i<$indexType; $i++) {
							$curPath .= substr($intPart, $i, 1).'/';
						}
					}
					if (is_file($curPath.$condition['name'].'.db')) {

				        if ($isInvalidData) {
							$fp = fopen($curPath.$condition['name'].'.db', 'r+');
							flock($fp, LOCK_EX);
						} else {
							$fp = fopen($curPath.$condition['name'].'.db', 'r');
							flock($fp, LOCK_SH);
						}
				        while (!feof($fp)) {
				            $str = fgets($fp);
				            if (strpos($str, $number) === false) continue;
				            $splitArr = explode('|', $str);
				            if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
				            $dataIndexKey = $splitArr[0];
				            $dbKey = $splitArr[1];
				            $tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];
				            if ($number == $dataIndexKey) {
				                $this->searchKeyArr[] = "$tableSuffix|$dbKey|{$condition['condition']}|{$condition['name']}";
				            }
				            if ($isInvalidData) {
				                fseek($fp, -2, SEEK_CUR);
				                fwrite($fp, 1, 1);
				                fseek($fp, 1, SEEK_CUR);
				            }
				        }
				    
				        flock($fp, LOCK_UN);
				        fclose($fp);
				    }
	    		}
	    	}
	    }
	}

	private function foreachNumberIndex($dirPath, $condition, $dirIndex=0, $isInvalidData=false) {
		if (!empty($condition)) {
			$paths = glob($dirPath.'*');
			if (!empty($paths)) {
				foreach ($paths as $path) {
					$curName = basename($path, '.db');
					if (is_dir($path)) {
						//验证首位
						$con = array();
			    		if (isset($condition['type']['>']) || isset($condition['type']['>='])) {
		    				//获取首位数
		    				$number = isset($condition['type']['>']) ? $condition['type']['>'] : $condition['type']['>='];
		    				$byte = substr($number, $dirIndex, 1);
		    				$con[] = '>='.(empty($byte) ? 0 : $byte);
		    			}
		    			if (isset($condition['type']['<']) || isset($condition['type']['<='])) {
		    				//获取首位数
		    				$number = isset($condition['type']['<']) ? $condition['type']['<'] : $condition['type']['<='];
		    				$byte = substr($number, $dirIndex, 1);
		    				$con[] = '<='.(empty($byte) ? 0 : $byte);
		    			}
		    			$flag = true;
		    			if (!empty($con)) {
		    				//首位验证
		    				foreach ($con as $conValue) {
		    					if (strlen($curName) < $dirIndex+1) {
		    						$tmpName = 0;
		    					} else {
									$tmpName = substr($curName, $dirIndex, 1);
		    					}
		    					$compare = $tmpName.$conValue;
			    				eval("\$compareRes=$compare;");
				    			if (!$compareRes) $flag = false;
		    				}
		    			}
		    			if ($flag) $this->foreachNumberIndex($path.'/', $condition, ++$dirIndex,$isInvalidData);

					} else {
						$name = $condition['name'];
						if ($curName != $name) continue;
						//组装条件
						$con = array();
						if (!empty($condition['type'])) {
							foreach ($condition['type'] as $key => $value) {
								$con[] = $key.$value;
							}
						}
						$type = $condition['condition'];

						if ($isInvalidData) {
							$fp = fopen($path, 'r+');
							flock($fp, LOCK_EX);
						} else {
							$fp = fopen($path, 'r');
							flock($fp, LOCK_SH);
						}

						while (!feof($fp)) {
							$str = fgets($fp);
							if (empty($str)) continue;
							$splitArr = explode('|', $str);
							if (isset($splitArr[3]) && trim($splitArr[3]) == '1') continue;
							$dataIndexKey = $splitArr[0];
							$dbKey = $splitArr[1];
							$tableSuffix = !isset($splitArr[2]) || empty($splitArr[2]) ? '@' : $splitArr[2];

							$flag = true;
							//验证条件
							if (!empty($con)) {
								foreach ($con as $conValue) {
									$compare = $dataIndexKey.$conValue;
				    				eval("\$compareRes=$compare;");
					    			if (!$compareRes) $flag = false;
								}
							}
							if ($flag) {
								$this->searchKeyArr[] = "$tableSuffix|$dbKey|{$condition['condition']}|{$condition['name']}";

							}

							if ($isInvalidData) {
								fseek($fp, -2, SEEK_CUR);
								fwrite($fp, 1, 1);
								fseek($fp, 1, SEEK_CUR);
							}
						}
					
						flock($fp, LOCK_UN);
						fclose($fp);
					}
				}
			} 
		}
	}
}

class PStorage extends Stackable {
    public function run(){}
}

}
