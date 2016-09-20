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
 */
class ZJRDB_V1 {
	private $base_path;
	private $trans_path;
	private $filter_path;
	private $data_index_path;
	private $index_path;
	private $index_type = 1;
	private $db_type = 'nosql';
    private $file_path;
    private $collect_time = 'COLLECT_TIME_#14%1&33|b';
    private $conf_max_size = 1023;
    private $issetHead = false;
    private $file_config = array();
    private $collect_stamp = 604800; //7天时间
    private $space = '';
    private $table = '';
    private $handle = null;
    private $transaction_data = array();
    private $isset_transaction = false;
    private $searchTreeData = array();	//缓存遍历树数据
    private $cacheIndexData = array();	//缓存索引数据
//     private $set_lock   = null;
	public function __construct($space, $table='') {
		if (empty($space)) return false;
		$this->base_path = WEB_PATH . 'cache/db/';
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
	    $this->trans_path = $this->base_path .$this->space. '/trans/';
	    $this->filter_path = $this->base_path .$this->space. '/filter/';
	    $this->index_path = $this->base_path .$this->space. '/index/';
	    $this->data_index_path = $this->base_path .$this->space. '/dataIndex/'. $this->table. '/';
	    $this->file_path = $this->base_path .$this->space. '/'.$this->table.'.db';
	    
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
		$this->table = $table;
		$this->init();
		if (is_file($this->file_path) && filesize($this->file_path) > $this->conf_max_size) {
			return false;
		}
		//创建数据索引文件夹
		if (!is_dir($this->data_index_path) && $type=='RDS') {
			$this->mkdirs($this->data_index_path);
		}
		$headStr = '@ZJRSYSKEY@|'.json_encode(array('next_filter_time' => time()+3600*24*7, 'lock' => 0, 'type' => $type));
		$left = $this->conf_max_size - strlen($headStr);
		if ($left > 0) {
			for ($i=0; $i<$left; $i++) {
				$headStr .= ' ';
			}
		}
		file_put_contents($this->file_path, $headStr.chr(10));
		return true;
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
	
	private function insert_core($keyValArr) {
		$nowTime = time();
		$result = array();
		//将文件指针移到末尾
		fseek($this->handle, 0, SEEK_END);
		if (!empty($keyValArr)) {
			foreach ($keyValArr as $v) {
				if (empty($v) || empty($v[0])) continue;
				$invalidTime = empty($v[2]) ? 0 : ($nowTime + intval($v[2]));
				$tmp1 = $v[0].'|'.$invalidTime.'|';
				$tmp2 = '|'.$v[1];
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
						} else {
							//插入成功开始创建索引
							$this->createIndex(array($v[0] => $curTell));
						}
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
		}
	}

	private function seekValByIndex($keys, $isInvalidData=false) {
		$data = array();
		$indexData = $this->getIndex($keys);
		if (!empty($indexData)) {
			$data = $this->seekDataByIndex($indexData);
			//如果设置了过期数据开关
			if ($isInvalidData) {
				if ($this->isset_transaction == false) {
					$this->setInvalidKey($indexData);
				} else {
					$this->transaction_data['delete'][] = $indexData;
				}
			}
		}
		return $data;
	}
	
	private function seekDataByIndex($indexData) {
		$data = array();
		$curTime = time();
		if (!empty($indexData)) {
			foreach ($indexData as $key => $index) {
				if (!empty($index)) {
					if (!empty($index)) {
						fseek($this->handle, intval($index));
						$tmpData = fgets($this->handle);
						$tmpArr = explode('|', $tmpData);
						if (count($tmpArr) == 4 && ($tmpArr[1] > $curTime || $tmpArr[1] == '0')) {
							$data[$key] = $tmpArr[3];
						} elseif (isset($data[$key])) {
							unset($data[$key]);
						}
					}
				}
			}
		}
		return $data;
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
	    							$data[$key][$seekKey] = $tmpArr[3];
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
		if (empty($this->file_path)) return false;
		$handle = fopen($this->file_path, 'r');
		$headStr = fgets($handle);
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
					file_put_contents($path.$this->table.'.db', $key.'|'.$index."\r\n", FILE_APPEND);
				}
			}
		}
	}

	/**
	 * 获取索引
	 * zjr
	 */
	private function getIndex($keys) {
		$keys = array_unique($keys);
		$keyArr = array();
		if (!empty($keys)) {
			foreach ($keys as $key) {
				$path = $this->index_path;
				if (!empty($key)) {
					$hashKey = md5($key);
					for ($i=0;$i<$this->index_type;$i++) {
						$path .= substr($hashKey, $i, 1).'/';
					}
					if (is_file($path.$this->table.'.db')) {

						$fp = fopen($path.$this->table.'.db', 'r');
						flock($fp, LOCK_SH);

						while (!feof($fp)) {
							$str = fgets($fp);
							if (strpos($str, $key) === false) continue;
							$splitPos = strpos($str, '|');
							$dbKey = substr($str, 0, $splitPos);
							if ($key == $dbKey) {
								$keyArr[$key] = substr($str, $splitPos+1);
							}
						}
						
						flock($fp, LOCK_UN);
						fclose($fp);
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
				file_put_contents($this->base_path, $this->file_path."\r\n", FILE_APPEND);
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
	public function select($filed, $where, $deep = '1,1', $page = 1, $prepage = '100', $sort = ''){
		//数据验证
		if (!is_array($where)) return false;
		if (empty($this->file_path) || $this->file_config['type'] != 'RDS') return false;
		$this->handle = fopen($this->file_path, 'r');
		
		flock($this->handle, LOCK_SH);
		//首先获取所有的索引数据
		$dataIndex = $this->get_data_index($where);
		
		$indexs = array_slice($dataIndex, ($page-1)*$prepage, $prepage);
		
		//获取根据索引获取数据
		$sourceData = $this->seekDataByIndex($indexs);
		
		//对数据进行处理
		$data = $this->handle_datas($sourceData, $filed, $deep);
		
		flock($this->handle, LOCK_UN);
		fclose($this->handle);

		unset($dataIndex);
		unset($sourceData);
		unset($this->handle);
		return $data;
	}
	
	/**
	 * 更新
	 */
	public function update($data, $where, $deep='1,1'){
		if (!is_array($where)) return false;
		if (empty($this->file_path) || $this->file_config['type'] != 'RDS') return false;
		$retData = array();
		$this->handle = fopen($this->file_path, 'r+');
		//先读取索引数据
		$dataIndex = $this->get_data_index($where);

		//根据索引获取数据	并且打开数据过期开关
		$sourceData = $this->seekValByIndex($dataIndex, true);
		
		flock($this->handle, LOCK_EX);
		//重组数据后重新生成
		$retData = $this->rebuild_update_datas($sourceData, $data, $deep);

		flock($this->handle, LOCK_UN);
		fclose($this->handle);

		unset($dataIndex);
		unset($sourceData);
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
		$this->handle = fopen($this->file_path, 'r+');
		//先读取索引数据
		$dataIndex = $this->get_data_index($where);

		//根据索引获取数据	并且打开数据过期开关
		$sourceData = $this->seekValByIndex($dataIndex, true);
		flock($this->handle, LOCK_UN);
		fclose($this->handle);
		
		unset($dataIndex);
		unset($sourceData);
		unset($this->handle);
		return true;
	}

	/**
	 * 构建数据索引树
	 * zjr
	 */
	private function createDataIndex($key, $data) {
		$data = is_array($data) ? $data : json_decode($data, true);
		if (!empty($data)) {
			if (is_array($data)) {
				foreach ($data as $name => $value) {
					if (is_numeric($name) || empty($name)) continue;
					//为name 键文件夹
					if (!empty($value)) {
						if (is_array($value)) {
							$this->createDataIndex($key, $value);
						} else {
							//为值创建索引目录
							$hashVal = md5($value);
							$path = $this->data_index_path;
							for ($i=0;$i<$this->index_type;$i++) {
								$path .= substr($hashVal, $i, 1).'/';
								if (!is_dir($path)) mkdir($path);
							}
							file_put_contents($path.$name.'.db', $hashVal.'|'.$key."\r\n", FILE_APPEND);
						}
					}
				}
			} else {
				//为值创建索引目录
				$hashVal = md5($data);
				$path = $this->data_index_path;
				for ($i=0;$i<$this->index_type;$i++) {
					$path .= substr($hashVal, $i, 1).'/';
					if (!is_dir($path)) mkdir($path);
				}
				file_put_contents($path.'_common.db', $hashVal.'|'.$key."\r\n", FILE_APPEND);
			}
		}
	}
	
	private function insert_rds($data) {
		if (!empty($data)) {
			$key = (microtime(true) *10000) . mt_rand(10, 99);
			$this->insert_core(array(array($key, json_encode($data), 0)));
			return $key;
		}
		return false;
	}
	
	
	private function get_data_index($where) {
		$cacheKey = md5(json_encode($where));
		if (isset($this->cacheIndexData[$cacheKey]) && !empty($this->cacheIndexData[$cacheKey])) {
			return $this->cacheIndexData[$cacheKey];
		}
		$keyArr = array();
		$resultKeyArr = array();
		if (isset($where['@id'])) {
			$resultKeyArr = array_flip($where['@id']);
			unset($where['@id']);
		}
		$condition = array();
		if (!empty($where)) {
			foreach ($where as $nameStr => $val) {
				if (!empty($nameStr) && !empty($val)) {
					$nameArr = explode('@', $nameStr);
					$name = $nameArr[0];
					$type = !empty($nameArr[1]) ? strtolower($nameArr[1]) : 'and';
					if (is_numeric($name)) continue;
					$condition[$type][$name] = $name;
					$hashVal = md5($val);
					$path = $this->data_index_path;
					for ($i=0;$i<$this->index_type;$i++) {
						$path .= substr($hashVal, $i, 1).'/';
					}
					if (is_file($path.$name.'.db')) {
						
						$fp = fopen($path.$name.'.db', 'r');
						flock($fp, LOCK_SH);
						
						while (!feof($fp)) {
							$str = fgets($fp);
							if (strpos($str, $hashVal) === false) continue;
							$splitPos = strpos($str, '|');
							$dataIndexKey = substr($str, 0, $splitPos);
							$dbKey = trim(substr($str, $splitPos+1));
							if ($hashVal == $dataIndexKey) {
								$keyArr[$dbKey][$type][$name] = 1;
							}
						}
						
						flock($fp, LOCK_UN);
						fclose($fp);
					}
				}
			}
		}
		if (!empty($keyArr)) {
			foreach ($keyArr as $dbKey => $names) {
				if ((isset($names['or']) && count($name['or']) > 0) || (isset($names['and']) && count($names['and']) == count($condition['and']))) {
					$resultKeyArr[$dbKey] = 1;
				}
			}
		}
		$keyArr = !empty($resultKeyArr) ? array_keys($resultKeyArr) : array();
		if (memory_get_usage(true) > 31457280) $this->cacheIndexData = null;	//设置了最大缓存30M时会清空缓存
		if (!empty($keyArr)) {
			$this->cacheIndexData[$cacheKey] = $keyArr;
		}
		return $keyArr;
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
					$retData[$index] = json_decode($val, true);
				} else {
					$this->searchTree(json_decode($val, true), $filed, 1, $minDeep, $maxDeep);
					if (!empty($this->searchTreeData)) {
						$retData[$index] = $this->searchTreeData;
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
					$this->searchTreeData[$name.'_'.$curDeep] = $val;
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
