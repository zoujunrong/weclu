<?php
/**
 * @description 文件数据库类
 * @author zjr
 * @create time 2015/10/29
 * @V2.1
 * @update time 2016/3/07
 * @description
 * V1.0 说明： key-val 数据库，文件每次都是全部读全部写，建议不同功能指定不同空间和不同表
 * V2.0 优化支持海量数据高效读取，支持事务，主要用于统计类, 日志类场景
 * 测试效率： 查询：耗时一秒查询支持数据量， 740000 * 16^索引级别， 支持32级索引。 插入：单条插入速度：0.0060s
 */
include 'zjrdb_conf.php';
class ZJRDB {
	private $base_path = ZJRDB_BASE_PATH;
	private $trans_path;
	private $filter_path;
	private $data_index_path;
	private $number_index_path;
	private $index_path;
	private $index_type = ZJRDB_INDEX_TYPE;
	private $db_type = 'nosql';
    private $file_path;
    private $conf_max_size = 1023;	//1023 byte
    private $table_file_max_size = ZJRDB_TABLE_FILE_MAX_SIZE;	//150M
    private $table_suffix = '';
    private $issetHead = false;
    private $file_config = array();
    private $collect_stamp = ZJRDB_DATA_COLLECT_STAMP; //7天时间
    private $space = '';
    private $table = '';
    private $handle = null;
    private $transaction_data = array();
    private $isset_transaction = false;
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
	    $this->trans_path = $this->base_path .$this->space. '/@trans/';
	    $this->filter_path = $this->base_path .$this->space. '/@filter/';
	    $this->index_path = $this->base_path .$this->space. '/@index/';
	    $this->data_index_path = $this->base_path .$this->space. '/@dataIndex/'. $this->table. '/';
	    $this->number_index_path = $this->base_path .$this->space. '/@numberIndex/'. $this->table. '/';
	    $this->file_path = $this->base_path .$this->space. '/'.$this->table.'/'.$this->table.$this->table_suffix.'.db';
	    
	    $tmp_dir = dirname($this->file_path);
	    if (!is_dir($tmp_dir)) {
	        $this->mkdirs($tmp_dir);
	    }
	    //创建事务文件夹
	    if (!is_dir($this->trans_path)) {
	    	mkdir($this->trans_path);
	    }
	    //创过滤引文件夹
	    if (!is_dir($this->filter_path)) {
	    	mkdir($this->filter_path);
	    }
	    //创建索引文件夹
	    if (!is_dir($this->index_path)) {
	    	mkdir($this->index_path);
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
		if (is_file($this->file_path) && filesize($this->file_path) > $this->conf_max_size) {
			return false;
		}
		//创建数据索引文件夹
		if (!is_dir($this->data_index_path) && $type=='RDS') {
			$this->mkdirs($this->data_index_path);
		}
		//创建数字索引文件夹
		if (!is_dir($this->number_index_path) && $type=='RDS') {
			$this->mkdirs($this->number_index_path);
		}
		$headStr = '@ZJRSYSKEY@|'.json_encode(array('next_filter_time' => time()+3600*24*7, 'lock' => 0, 'type' => $type));
		$left = $this->conf_max_size - strlen($headStr);
		if ($left > 0) {
			for ($i=0; $i<$left; $i++) {
				$headStr .= ' ';
			}
		}
		file_put_contents($this->file_path, $headStr.chr(10));
		//写入文件配置信息
	    $this->initConfigInfo();
		return true;
	}
	
	private function changeTableSuffix($suffix='', $mode='r') {
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

			if (!$isVague) {
				$data = $this->seekValByIndex(array($key));
			} else {
				$data = $this->seekValByKey($key);
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

			$data = $this->seekValByIndex($keys);
			
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
		if (empty($this->file_path)) return false;
		$this->handle = fopen($this->file_path, 'r+');
		fclose($this->handle);
		$this->isset_transaction = true;
		$this->transaction_data = array();
	}
	
	/**
	 * 事务提交
	 */
	public function commit() {
		if (empty($this->file_path)) return false;
		$this->handle = fopen($this->file_path, 'r+');
		
		flock($this->handle, LOCK_EX);
		//开始写入数据
		$this->submitTransactionData();
		flock($this->handle, LOCK_UN);
		fclose($this->handle);
		
		unset($this->handle);
		$this->transaction_data = array();
		$this->isset_transaction = false;
	}
	
	/**
	 * 事务回滚
	 */
	public function rollback() {
		$this->transaction_data = array();
		$this->isset_transaction = false;
	}
	
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
	
	private function insert_core($keyValArr) {
		$nowTime = time();
		$result = array();
		//重置表
		$this->setLatestTable();
		//将文件指针移到末尾
		fseek($this->handle, 0, SEEK_END);
		if (!empty($keyValArr)) {
			foreach ($keyValArr as $v) {
				if (empty($v) || empty($v[0])) continue;
				$invalidTime = empty($v[2]) ? 0 : ($nowTime + intval($v[2]));
				$tmp1 = $v[0].'|'.$invalidTime.'|';
				$tmp2 = '|' . base64_encode( gzdeflate( $v[1] ) );
				$cols = strlen($tmp1)+strlen($tmp2)+1;
				$writeStr = $tmp1.$cols.$tmp2;
				$curTell = ftell($this->handle);
				//如果开启了事务
				if (!$this->isset_transaction) {
					if (fwrite($this->handle, $writeStr.chr(10), (strlen($writeStr)+1))) {
						$result[$v[0]] = $curTell;
						//如果是关系型数据库，则要创建数据索引
						if ($this->file_config['type'] == 'RDS') {
							$this->createDataIndex($curTell, $v[1]);
						} 
						//插入成功开始创建索引
						$this->createIndex(array($v[0] => $curTell));
						
					} else {
						$result[$v[0]] = 0;
					}
				} else {
					$this->transaction_data['insert'][$v[0]] = array('data' => $writeStr, 'index' => -1, 'invalid_time' => $invalidTime);
					$result[$v[0]] = -1;
				}
			}
		}
		return $result;
	}
	
	private function submitTransactionData() {
		//将文件指针移到末尾
		fseek($this->handle, 0, SEEK_END);
		$data = array();
		if ($this->isset_transaction) {
			if (!empty($this->transaction_data['insert'])) {
				foreach ($this->transaction_data['insert'] as $k => $v) {
					if (empty($v) || empty($v['data'])) continue;
					$curTell = ftell($this->handle);
					fwrite($this->handle, $v['data'].chr(10), (strlen($v['data'])+1));
					$data[$k] = $curTell;
					//如果是关系型数据库，则要创建数据索引
					if ($this->file_config['type'] == 'RDS') {
						$tmp = substr(strrchr($v['data'], '|'), 1);
						$this->createDataIndex($curTell, trim($tmp));
					}
				}
				if ($this->file_config['type'] == 'NOSQL') {
					//非关系型插入成功开始创建索引
					$this->createIndex($data);
				}
			}
			//删除数据事务
			if (!empty($this->transaction_data['delete'])) {
				foreach ($this->transaction_data['delete'] as $val) {
					$this->setInvalidKey($val);
				}
			}
		}
		return $data;
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
			if ($this->dataCount > 0 || $page==1) $curIndex = 0;
			else $curIndex = ($page-1)*$prepage - $this->searchCount;
			$currentCount = 0;
			foreach ($indexData as $index => $nums) {
				if (!empty($index)) {
					if (!empty($index)) {
						fseek($this->handle, intval($index));
						$tmpData = fgets($this->handle);
						$tmpArr = explode('|', $tmpData);
						$keyId = $tmpArr[0];
						if (count($tmpArr) == 4 && ($tmpArr[1] > $curTime || $tmpArr[1] == '0')) {
							$currentCount++;
							$this->searchCount++;
							if ($prepage > 0 && $currentCount <= $curIndex) continue;
							$this->seekData[$keyId] = gzinflate(base64_decode($tmpArr[3]));
							
							if ($isInvalidData) $invalidData[$keyId] = $index;
							$this->dataCount++;
							if ($prepage > 0 && $this->dataCount >= $prepage) break;
						} elseif (isset($this->seekData[$keyId])) {
							unset($this->seekData[$keyId]);
						}
					}
				}
			}
			//如果设置了过期数据开关
			if ($isInvalidData && !empty($invalidData)) {
				$this->setInvalidKey($invalidData);
			}
		}
	}
	
	private function seekValByKey($key = '') {
		$data = array();
		$curTime = time();
		$rowIndex = 0;	//行指针
	    if ($this->handle !== false) {
	    	$tmpStr = '';
	    	if (!empty($key)) {
	    		while (!feof($this->handle)) {
	    			if ($rowIndex == 0) {	//设置系统信息
	    				$this->rebuildConfig(fgets($this->handle));
	    				$rowIndex = ftell($this->handle);
	    			} else {
	    				$tmpStr = fgets($this->handle);

	    				if (!empty($tmpStr)) {

	    					$splitPos = strpos($tmpStr, '|');
							$seekKey = substr($tmpStr, 0, $splitPos);
							if ($this->checkKeyIsSeek($key, $seekKey)) {
								$tmpArr = explode('|', $tmpStr);
								//判断数据是否过期
		    					if ($tmpArr[1] > $curTime || $tmpArr[1] == 0) {
		    						//判断key是否找到
	    							$data[$key][$seekKey] = gzinflate(base64_decode($tmpArr[3]));
		    					} elseif (isset($data[$key][$seekKey])) {
		    						unset($data[$key][$seekKey]);
		    					}
							}

	    				}

	    			}
	    		}
	    	}
	    	unset($tmpStr);
	    }
	    return $data;
	}
	
	private function rebuildConfig($str) {
		$str = trim($str);
		if (!empty($str) && empty($this->file_config)) {
			$tmpArr = explode('|', $str);
			if (!empty($tmpArr) && count($tmpArr) == 2) {
				$this->file_config = json_decode($tmpArr[1], true);
			}
		}
	}
	
	private function changeConfigInfo($key, $val) {
		if (!empty($this->file_config) && isset($this->file_config[$key])) {
			$this->file_config[$key] = $val;
			//设置信息
			fseek($this->handle, 12);
			$str = json_encode($this->file_config);
			fwrite($this->handle, $str, strlen($str));
		}
	}
	
	private function initConfigInfo() {
		if (!is_file($this->file_path)) return false;
		$reTimes = 0;
		do {
			$handle = fopen($this->file_path, 'r');
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
			$this->insert_core(array(array($key, json_encode($data), 0)));
			return $key;
		}
		return false;
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
		if (is_numeric($name)) continue;
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
	    if (empty($condition)) continue;
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
		if (is_numeric($name)) continue;
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
	    if (empty($condition)) continue;
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
