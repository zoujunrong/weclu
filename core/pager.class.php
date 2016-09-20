<?php
/**
 * @description 页面操作层
 * @author zjr
 * @2016/08/02
 * 说明： 数据库页面操作层
 */
include_once WECLU_CORE_PATH . 'storage.class.php';
include_once WECLU_CORE_PATH . 'index.class.php';
include_once WECLU_CORE_PATH . 'function.php';

class Pager {
    private $storageObj = null;
    private $indexObj 	= null;
    private $space 		= null;
    private $table		= null;
    private $type		= null;
	public function __construct( $space ) {
		$this->storageObj = new Storage( $space );
		$this->space = $space;
	}
	
	public function change_space( $space ) {
	    unset( $this -> storageObj );
	    unset( $this -> indexObj );
	    $this->storageObj = new Storage( $space );
	    $this->space = $space;
	    $this->table = null;
	    $this->type  = null;
	}
	
	private function select_table( $table ) {
		if ( $table != $this->table ) {
			//选择表
			$this->storageObj->select_table( $table );
			//获取表信息
			$tableInfo = $this->storageObj->get_header();
			if ( isset( $tableInfo['type'] ) && ! empty( $tableInfo['type'] ) ) {
			    if ( $this->type == $tableInfo['type'] ) return true;
			    $this -> indexObj = null;
			    
				switch ( $tableInfo['type'] ) {
					case 'HASH' :
						$this -> indexObj = new HashIndex( $this->space );
						break;
					case 'RDS' :
						$this -> indexObj = new BtreeIndex( $this->space );
						//切换索引表
						$this->indexObj->select_table( $table );
						break;
					default:
						throw new Exception("unkown table engine!");
				}
				$this->type = $tableInfo['type'];
			} else {
				throw new Exception("unkown table engine!");
			}
		}
		return true;
	}
	
	public function create( $table, $table_type, $start_id ) {
	    $this -> storageObj->create_table( $table, $table_type, $start_id );
	}
	
	public function show_databases() {
	    $path = WECLU_BASE_PATH . 'data/';
	    $dbs = array();
	    if ( is_dir( $path ) ) {
	        $dbs = glob( $path . '*' );
	        if ( ! empty( $dbs ) ) {
	            foreach ( $dbs as &$db ) {
	                $db = basename( $db );
	            }
	        }
	    }
	    return $dbs;
	}
	
	public function show_tables() {
	    $path = WECLU_BASE_PATH . 'data/' . $this->space . '/';
	    $tables = array();
	    if ( is_dir( $path ) ) {
	        $table_paths = glob( $path . '*' );
	        if ( ! empty( $table_paths ) ) {
	            foreach ( $table_paths as &$table_path ) {
	                $table = strstr( basename( $table_path ), '_', true );
	                $tables[$table] = 1;
	            }
	            $tables = array_keys( $tables );
	        }
	    }
	    return $tables;
	}
	
	/***********************************************************************************************************
	 * HASH引擎
	 */
	
	/**
	 * 初始化hash空间
	 */
	public function init_hash_space() {
		$hash_path = WECLU_BASE_PATH . 'index/' . $this->space . '/';
		if ( ! is_dir( $hash_path ) ) weclu_mkdirs( $hash_path );
		$hash_path .= 'hash.index';
		if ( ! is_file( $hash_path ) ) {
			$handle = fopen( $hash_path, 'w+' );
			do {
				$endPos = ftell( $handle );
				fwrite( $handle, str_pad( ' ', WECLU_HASH_BLOCK_SIZE * 1024 , ' ' ), WECLU_HASH_BLOCK_SIZE * 1024 );
			} while( $endPos < WECLU_HASH_BLOCK_RULES );
		}
	}
	
	/**
	 * 设置hash引擎
	 */
	public function sets( $table, $keyVal ) {
		$retData = array();
		$this -> select_table( $table );
		$prefixLen = strlen( $table ) + 1;
		if ( $this->type == 'HASH' ) {
			foreach ( $keyVal as $key => $val ) {
			    $key = $table . ':' . $key;
			    $posArr = $this->indexObj->get_index( array( $key ) );
				if ( ! is_array( $val ) ) $val = array( $val, 0 );
				if ( isset( $posArr[$key] ) ) {
					$id = $this->storageObj->update_main( $posArr[$key], array( $key => $val[0], '@expire' => intval( $val[1] ) ) );
					if ( ! empty( $id ) ) {
					    $retKey = substr( $key, $prefixLen );
						$retData[$retKey] = $id;
					}
				} else {
					$id = $this->storageObj->insert_main( array( $key => $val[0], '@expire' => intval( $val[1] ) ) );
					if ( ! empty( $id ) ) {
						$keyArr[$key] = $id;
						$retKey = substr( $key, $prefixLen );
						$retData[$retKey] = $id;
					}
				}
			}
			if ( ! empty( $keyArr ) ) {
				$this->indexObj->set_index( $keyArr );
			}
		} else {
			throw new Exception("no match engine HASH in table '{$table}' !");
		}
		return $retData;
	}
	
	/**
	 * 设置hash引擎
	 */
	public function gets( $table, $keys ) {
		$retData = array();
		$time = time();
		$this -> select_table( $table );
		$prefixLen = strlen( $table ) + 1;
		if ( $this->type == 'HASH' && ! empty( $keys ) ) {
		    foreach ( $keys as &$key ) {
		        $key = $table . ':' . $key;
		    }
			$posArr = $this->indexObj->get_index( $keys );
			if ( ! empty( $posArr ) ) {
				foreach ( $posArr as $key => $id ) {
					//获取数据
					$data = $this->storageObj->fetch_main( $id );
					if ( $data['@expire'] == 0 || $data['@expire'] > $time ) {
					    $retKey = substr( $key, $prefixLen );
						$retData[$retKey] = $data[$key];
					}
				}
			}
		} else {
			throw new Exception("no match engine HASH in table '{$table}' !");
		}
		return $retData;
	}
	
	/***********************************************************************************************************
	 * RDS引擎
	 */
	
	/**
	 * 创建索引
	 * @param string $table
	 * @param array $index_arr
	 */
	public function create_index( $table, $index_arr ) {
		$keyArr = array();
	    if ( ! empty( $index_arr ) ) {
	        $this -> select_table( $table );
	        if ( $this->type == 'RDS' ) {
	            $keyArr = $this->indexObj->create_index( $index_arr );
	            //获取表信息
	            $tableInfo = $this->storageObj->get_header();
	            //判断有没有数据
	            if ( $tableInfo['n_id'] > 0 ) {
	            	$idArr = $this->storageObj->fetch_ids( 1, $tableInfo['n_id'] );
	            	if ( ! empty( $idArr ) ) {
	            		$prepage = 1000;
	            		$page = ceil( count( $idArr ) / $prepage );
	            		for ( $i=1; $i<=$page; $i++ ) {
	            			$ids = array_slice( $idArr, ($i-1)*$prepage, $prepage );
	            			$datas = $this->storageObj->fetch_main_batch( $ids );
	            			if ( ! empty( $datas ) ) {
	            				foreach ( $datas as $id => $data ) {
	            					$retData = array();
	            					$tmpkeyArr = $keyArr;
	            					//建立索引
	            					$this -> get_key_arr( $tmpkeyArr, $retData, $data );
	            					$this->indexObj->set_index( $retData, $id );
	            				}
	            			}
	            		}
	            	}
	            }
	        }
	    }
	    return $keyArr;
	}
	/**
	 * 删除索引
	 */
	public function delete_index( $table, $index_arr ) {
		if ( ! empty( $index_arr ) ) {
			$this -> select_table( $table );
			return $this->indexObj->delete_index( $index_arr );
		}
		return array();
	}
	/*
	 * 插入
	 */
	public function insert( $table, $data ) {
		$this -> select_table( $table );
		$keyArr = $this->indexObj->get_all_index_key();
		$keyArr = array();
		if ( $this->type == 'RDS' ) {
		    $id = $this->storageObj->insert_main( $data );
		    $keyArr = $this->indexObj->get_all_index_key();
		    $retData = array();
		    //建立索引
		    $this -> get_key_arr( $keyArr, $retData, $data );
		    $this->indexObj->set_index( $retData, $id );
		} else {
			throw new Exception("no match engine RDS in table '{$table}' !");
		}
		return $id;
	}
	
	public function select( $table, $filed, $where, $sort, $page, $prepage ) {
		$retData = array();
		$this -> select_table( $table );
	    //先通过索引查找到索引信息
	    $indexs = $this->indexObj->get_index( $where, $this->storageObj );
	    
	    //基于id的统计
	    if ( isset( $filed['count(*)'] ) ) {
	    	$count = count( $indexs );
	    	$indexs = array( reset( $indexs ) );
	    }
	    
	    //基于id的排序
	    if ( ! empty( $sort ) ) {
	    	if ( key( $sort ) == '_id' ) {
	    		$sortFlag = strtolower( reset( $sort ) );
	    		if ( $sortFlag == 'desc' ) $indexs = array_reverse( $indexs );
	    		if ( $page > 0 ) $indexs = array_slice( $indexs, ( $page - 1 ) * $prepage, $prepage );
	    	}
	    } else {
	    	if ( $page > 0 ) $indexs = array_slice( $indexs, ( $page - 1 ) * $prepage, $prepage );
	    }
	    
	    $datas = $this->storageObj->fetch_main_batch( $indexs );
	    if ( ! empty( $datas ) ) {
	    	$isShowId = isset( $filed['_id'] ) || isset( $filed['*'] ) ? true : false;		//判断是否要显示ID
	    	foreach ( $datas as $id => $data ) {
	    	    $fetch_data = array();
	    	    if ( isset( $filed['count(*)'] ) ) {
	    	        $fetch_data['count(*)'] = $count;
	    	        unset( $filed['count(*)'] );
	    	    }
	    	    $fetchFiled = $filed;
	    	    $this -> get_filed_by_data( $fetchFiled, $data, $fetch_data );
	    	    if ( ! empty( $fetch_data ) ) {
	    	        if ( $isShowId ) $fetch_data['_id'] = $id;
	    	        $retData[] = $fetch_data;
	    	    }
	    	}
	    }
	    
	    //基于其他字段的排序
	    if ( ! isset( $sortFlag ) && ! empty( $sort ) ) {
	    	$args = array( &$retData );
	    	$hasSort = false;
	    	foreach ( $sort as $k => $v ) {
	    		if ( isset( $retData[0][$k] ) ) {
	    			$sortFlag = strtolower( $v );
	    			if ( $sortFlag == 'desc' ) $sortFlag = SORT_DESC;
	    			else $sortFlag = SORT_ASC;
	    			$args[] = $k;
	    			$args[] = $sortFlag;
	    			$hasSort = true;
	    		}
	    	}
	    	if ( $hasSort ) {
	    		$retData = weclu_array_orderby( $args );
	    	}
	    	if ( $page > 0 ) $retData = array_slice( $retData, ( $page - 1 ) * $prepage, $prepage );
	    }
	    
	    return $retData;
	}
	
	public function update( $table, $updateData, $where ) {
	    $updateIds = array();
		$this -> select_table( $table );
		//获取所有索引数据
		$keyArr = $this->indexObj->get_all_index_key();
		//先通过索引查找到索引信息
		$ids = $this->indexObj->get_index( $where, $this->storageObj );
		if ( ! empty( $ids ) ) {
			foreach ( $ids as $id ) {
			    $id = base_convert( $id, 32, 10 );	//将32 进制 转换成10进制
			    $sourceData = $this->storageObj->fetch_main( $id );
			    $deleteData = array();
			    $tmpkeyArr = $keyArr;
			    $this -> set_new_data( $sourceData, $updateData, $deleteData );
				$this->storageObj->update_main( $id, $sourceData );
				//删除索引信息
				$retData = array();
				$this -> get_key_arr( $tmpkeyArr, $retData, $deleteData );
				$this->indexObj->del_index( $retData, $id );
				//新增索引信息
				$tmpkeyArr = $keyArr;
				$retData = array();
				$this -> get_key_arr( $tmpkeyArr, $retData, $updateData );
				$this->indexObj->set_index( $retData, $id );
				$updateIds[] = $id;
			}
		}
		return $updateIds;
	}
	
	public function delete( $table, $where ) {
	    $delIds = array();
	    $this -> select_table( $table );
	    //获取所有索引数据
	    $keyArr = $this->indexObj->get_all_index_key();
	    //先通过索引查找到索引信息
	    $ids = $this->indexObj->get_index( $where, $this->storageObj );
	    if ( ! empty( $ids ) ) {
	        foreach ( $ids as $id ) {
	        	$tmpkeyArr = $keyArr;
	            //查询数据
	        	$id = base_convert( $id, 32, 10 );	//将32 进制 转换成10进制
	            $data = $this->storageObj->fetch_main( $id );
	            $this->storageObj->delete_main( $id );
	            $retData = array();
	            $this -> get_key_arr( $tmpkeyArr, $retData, $data );
	            //删除索引信息
	            $this->indexObj->del_index( $retData, $id );
	            $delIds[] = $id;
	        }
	    }
	    return $delIds;
	}
	
	/**
	 * 组装索引键值
	 */
	private function get_key_arr( &$keyArr, &$retData, $dataArr, $prefix='' ) {
	    if ( ! empty( $dataArr ) && ! empty( $keyArr ) ) {
	        foreach ( $dataArr as $key => $val ) {
	            $keyPath = $prefix;
	            if ( ! is_numeric( $key ) ) {
	                if ( $prefix ) $keyPath .= '.';
	                $keyPath .= $key;
	            }
	            $keyStr = strlen( $keyPath ) > 32 ? md5( $keyPath ) : $keyPath;
	            if ( isset( $keyArr[$keyStr] ) ) {
	                unset( $keyArr[$keyStr] );
	                if ( is_array( $val ) ) {
	                    foreach ( $val as $k => $v ) {
	                        if ( ! is_numeric( $k ) ) {
	                            $retData[$keyStr][] = $k;
	                        } elseif ( ! is_array( $v ) ) {
	                            $retData[$keyStr][] = $v;
	                        }
	                    }
	                } else {
	                    $retData[$keyStr][] = $val;
	                }
	                $retData[$keyStr] = array_unique( $retData[$keyStr] );
	            }
	            if ( is_array( $val ) ) {
	                $this -> get_key_arr( $keyArr, $retData, $val, $keyPath );
	            }
	        }
	    }
	}
	
	/**
	 * 获取数据中的需要的字段
	 * @param unknown $filed
	 * @param unknown $data
	 * @param unknown $retData
	 * @param string $prefix
	 */
	private function get_filed_by_data( &$filed, $data, &$retData, $prefix='' ) {
		if ( ! empty( $data ) ) {
			if ( isset( $filed['*'] ) ) {
				unset( $filed['*'] );
				$retData = array_merge( $retData, $data );
			} else {
				foreach ( $data as $key => $val ) {
					if ( empty( $filed ) ) break;
					$keyPath = $prefix;
					if ( ! is_numeric( $key ) ) {
						if ( $prefix ) $keyPath .= '.';
						$keyPath .= $key;
					}
					//判断是否需要取
					if ( isset( $filed[$keyPath] ) ) {
						$retData[$keyPath] = $val;
						unset( $filed[$keyPath] );
					}
					if ( is_array( $val ) ) {
						$this -> get_filed_by_data( $filed, $val, $retData, $keyPath );
					}
				}
			}
		}
	}
	
	private function set_new_data( &$sourceData, &$updateData, &$deleteData, $prefix='' ) {
    	if ( ! empty( $sourceData ) && ! empty( $updateData ) ) {
    		foreach ( $sourceData as $key => &$val ) {
    			$keyPath = $prefix;
    			if ( ! is_numeric( $key ) ) {
    				if ( $prefix ) $keyPath .= '.';
    				$keyPath .= $key;
    			}
    			if ( isset( $updateData[$keyPath] ) ) {
    			    $deleteData[$keyPath] = $val;
    				$sourceData[$key] = $updateData[$keyPath];
    			} elseif ( is_array( $val ) ) {
    				$this -> set_new_data( $val, $updateData, $deleteData, $keyPath );
    			}
    		}
    	}
    }
	
}
