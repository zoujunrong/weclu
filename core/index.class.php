<?php
/**
 * @description 索引管理层
 * @author zjr
 * @2016/08/02
 * 说明： 数据库索引抽象层
 */

class HashIndex {
	private $index_path = null;
	private $handle = null;
	private $assist_handle = null;
	public function __construct( $space ) {
		$this -> index_path = WECLU_BASE_PATH . 'index/' . $space . '/';
		if ( ! is_dir( $this->index_path ) ) weclu_mkdirs( $this->index_path );
	}
	
	public function __destruct() {
	    $this -> close();
	}
	
	/**
	 * 设置hash索引句柄
	 */
	public function set_hash_handle() {
		$hash_path = $this->index_path . 'hash.index';
		//关闭当前数据句柄
		if ( ! empty( $this -> handle ) ) return $this -> handle;
		if ( is_file( $hash_path ) ) {
			$this -> handle = fopen( $hash_path, 'r+' );
		} else {
			throw new Exception( 'no init hash storage space' );
		}
	}
	
	/**
	 * 设置hash数据辅助存储区句柄
	 */
	public function set_hash_assist_handle() {
	    $hash_assist_path = $this->index_path . 'hash_assist.index';
	    //关闭当前数据句柄
	    if ( ! empty( $this -> assist_handle ) ) return $this -> assist_handle;
	    if ( is_file( $hash_assist_path ) ) {
	        $this -> assist_handle = fopen( $hash_assist_path, 'r+' );
	    } else {
	        $this -> assist_handle = fopen( $hash_assist_path, 'w+' );
	    }
	}
	
	/**
	 * 关闭文件存储引擎 并解锁
	 * @param handle  文件句柄
	 */
	private function close( $handle = null ) {
	    if ( ! empty( $handle ) ) {
	        fclose( $handle );
	        unset( $handle );
	    }
	    else {
	        if ( ! empty( $this -> handle ) ) {
	            fclose( $this -> handle );
	            unset( $this -> handle );
	        }
	        if ( ! empty( $this -> assist_handle ) ) {
	            fclose( $this -> assist_handle );
	            unset( $this -> assist_handle );
	        }
	    }
	}
	
	/********************************************************************************************
	 * HASH 索引
	 * zjr
	 */
	
	/**
	 * 设置hash索引
	 */
	public function set_index( $keyArr ) {
		
	    if ( ! empty( $keyArr ) ) {
	        $this -> set_hash_handle();
	        foreach ( $keyArr as $key => $data_pos ) {
	            list( $pos, $index ) = weclu_hash( $key );
	            
	            //共享锁获取索引信息
	            flock( $this->handle, LOCK_EX );
	            
	            $mainArr = array();
	            $headArr = array( $pos );
	            fseek( $this->handle, $pos );
	            //读取
	            $str = trim( fread( $this->handle, WECLU_HASH_BLOCK_SIZE ) );
	            if ( ! empty( $str ) ) {
	                $main_data = trim( substr( $str, WECLU_HASH_HEAD_BLOCK_SIZE ) );
	                $head = base_convert( substr( $str, 0, WECLU_HASH_HEAD_BLOCK_SIZE ), 32, 10 );
	                while ( $head > 0 ) {
	                    $headArr[] = $head;
	                    fseek( $this->handle, $head );
	                    $str = trim( fread( $this->handle, WECLU_HASH_BLOCK_SIZE ) );
	                    if ( ! empty( $str ) ) {
	                        $main_data .= trim( substr( $str, WECLU_HASH_HEAD_BLOCK_SIZE ) );
	                        $head = base_convert( substr( $str, 0, WECLU_HASH_HEAD_BLOCK_SIZE ), 32, 10 );
	                    }
	                }
	                $mainArr = json_decode( $main_data, true );
	            }
	            if ( ! empty( $headArr ) ) {
	                $mainArr['n'] = $headArr;
	            }
	            
	            //插入索引数据
	            $this -> insert_new_hash( $index, $key, $data_pos, $mainArr);
	            
	            flock( $this->handle, LOCK_UN );
	        }
	    }
		
	}
	
	/**
	 * 查询索引信息
	 */
	public function get_index( $keyArr ) {
	    $posArr = array();
	    if ( ! empty( $keyArr ) ) {
	        $this -> set_hash_handle();
	        foreach ( $keyArr as $key ) {
	            list( $pos, $index ) = weclu_hash( $key );
	
	            $mainArr = array();
	            $headArr = array( $pos );
	            //共享锁获取索引信息
	            flock( $this->handle, LOCK_SH );
	            fseek( $this->handle, $pos );
	            //读取
	            $str = trim( fread( $this->handle, WECLU_HASH_BLOCK_SIZE ) );
	            if ( ! empty( $str ) ) {
	                $main_data = trim( substr( $str, WECLU_HASH_HEAD_BLOCK_SIZE ) );
	                $head = base_convert( substr( $str, 0, WECLU_HASH_HEAD_BLOCK_SIZE ), 32, 10 );
	                while ( $head > 0 ) {
	                    $headArr[] = $head;
	                    fseek( $this->handle, $head );
	                    $str = trim( fread( $this->handle, WECLU_HASH_BLOCK_SIZE ) );
	                    if ( ! empty( $str ) ) {
	                        $main_data .= trim( substr( $str, WECLU_HASH_HEAD_BLOCK_SIZE ) );
	                        $head = base_convert( substr( $str, 0, WECLU_HASH_HEAD_BLOCK_SIZE ), 32, 10 );
	                    }
	                }
	                $mainArr = json_decode( $main_data, true );
	            } else {
	            	flock( $this->handle, LOCK_UN );
	            	continue;
	            }
	
	            if ( ! empty( $headArr ) ) {
	                $mainArr['n'] = $headArr;
	            }
	
	            //获取数据落区
	            $locate = $this -> get_hash_locate_area( $index, $mainArr );
	            if ( $locate === null ) {  //落区主空间
	                if ( isset( $mainArr['d'][$index][$key] ) ) $posArr[$key] = $mainArr['d'][$index][$key];
	            } else {
	                //去辅助空间读取数据
	                list( , $data ) = $this -> get_hash_assist_data( $mainArr['m'][$locate] );
	                if ( isset( $data[$index][$key] ) ) $posArr[$key] = $data[$index][$key];
	            }
	            flock( $this->handle, LOCK_UN );
	        }
	    }
	    return $posArr;
	}
	
	/**
	 * hash主数据区内部结构
	 * m(map, 辅助数据区的映射), d(data, 主数据区存放的数据), n(next, 主数据区空间不足，新增的空间映射,此值存在表示数据发生了裂变)
	 * @param unknown $hash_val
	 * @param unknown $pos
	 */
	private function insert_new_hash( $hash_val, $key, $data_pos, &$mainArr ) {
	    $locate = $this -> get_hash_locate_area( $hash_val, $mainArr );
	    if ( $locate !== null ) {
            //去辅助数据空间取数据
            list( $headArr, $data ) = $this -> get_hash_assist_data( $mainArr['m'][$locate] );
            
            $data[$hash_val][$key] = $data_pos;
            
            $data_count = count( $data );
            $new_count = ceil( $data_count / 2 );  //新数组长度
            ksort( $data );   //重新排序
            
            //如果至少有两个不同的hash值
            if ( $data_count > 1 ) {
                
                //出栈元素的个数
                $out_nums = ceil( $data_count / 2 );
                $out_nums = $data_count - $out_nums < 1 ? 0 : $out_nums;    //如果只有一个则出栈
                $out_arr = array();
                while ( $out_nums > 0 ) {
                    $end_val = end( $data );
                    $end_key = key( $data );
                    $out_arr[$end_key] = $end_val;
                    unset( $data[$end_key] );
                    $out_nums--;
                }
                
                //如果$out_arr不为空 则分裂出了新hash槽
                if ( ! empty( $out_arr ) ) {
                    ksort( $out_arr );
                    list( $hash_index, $new_pos ) = $this -> change_hash_assiste_data( $out_arr ); //将新槽存储到辅助数据空间
                    
                    //将新槽注册到主空间
                    $mainArr['m'][$hash_index] = $new_pos;
                    ksort( $mainArr['m'] );
                    //修改主空间信息
                    $this -> change_hash_main( $mainArr );
                }
                //存储原有的hash槽
                $this -> change_hash_assiste_data( $data, $headArr );
            } else {
                $this -> change_hash_assiste_data( $data, $headArr );
            }
	    } else {
	        $mainArr['d'][$hash_val][$key] = $data_pos;
	        ksort( $mainArr['d'] );
	        //修改主空间信息
	        $this -> change_hash_main( $mainArr );
	    }
	    return true;
	}
	
	/**
	 * 主数据信息改变时，需要重新存储主空间
	 * @param unknown $mainArr
	 * @return boolean
	 */
	private function change_hash_main( &$mainArr ) {
	    $headArr = array();
	    if ( isset( $mainArr['n'] ) ) {
	        $headArr = $mainArr['n'];
	        unset( $mainArr['n'] );
	    }
	    if ( empty( $headArr ) ) return false;
	    
	    $main_json = json_encode( $mainArr );
	    $json_len = strlen( $main_json );
	    $head_count = count( $headArr );
	    $data_block_size = WECLU_HASH_BLOCK_SIZE - WECLU_HASH_HEAD_BLOCK_SIZE;     //数据块真实长度
	    if ( $head_count > 1 ) {
	        for ( $i=0; $i < $head_count; $i++ ) {
	            //判断是否是最后一个元素
	            if ( ( $i + 1 ) == $head_count ) {
	                //仍然有剩余数据
	                if ( $json_len > $head_count * $data_block_size ) {
	                    fseek( $this->handle, 0, SEEK_END );
	                    $headArr[$i+1] = ftell( $this->handle );
	                    if ( $headArr[$i+1] < WECLU_HASH_BLOCK_RULES ) {
	                        fseek( $this->handle, WECLU_HASH_BLOCK_RULES );
	                        $headArr[$i+1] = WECLU_HASH_BLOCK_RULES;
	                    }
	                    $input_str = str_pad( 0, WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . substr( $main_json, ( $i + 1 ) * $data_block_size );
	                    fwrite( $this->handle, str_pad( $input_str, WECLU_HASH_BLOCK_SIZE, ' ' ), WECLU_HASH_BLOCK_SIZE );
	                } else {
	                    $headArr[$i+1] = 0;
	                }
	            }
	            fseek( $this->handle, $headArr[$i] );
	            $input_str = str_pad( base_convert( $headArr[$i+1], 10, 32 ), WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . substr( $main_json, $i * $data_block_size, $data_block_size );
	            fwrite( $this->handle, str_pad( $input_str, WECLU_HASH_BLOCK_SIZE, ' ' ), WECLU_HASH_BLOCK_SIZE );
	        }
	    } elseif ( $json_len > $data_block_size ) {
	        if ( isset( $mainArr['d'] ) ) {
	            //出栈元素的个数
	            $data_count = count( $mainArr['d'] );
	            $out_nums = ceil( $data_count / 2 );
	            $out_nums = $data_count - $out_nums > 1 ? $out_nums : $data_count;    //如果只剩2个就全部出栈
	            $out_arr = array();
	            while ( $out_nums > 0 ) {
	                $end_val = end( $mainArr['d'] );
	                $end_key = key( $mainArr['d'] );
	                $out_arr[$end_key] = $end_val;
	                unset( $mainArr['d'][$end_key] );
	                $out_nums--;
	            }
	            if ( empty($mainArr['d'] ) ) {
	                unset( $mainArr['d'] );
	            }
	            //将出栈数据写入辅助数据空间，以新槽的方式新增
	            if ( ! empty( $out_arr ) ) {
	                
	                list( $hash_index, $new_pos ) = $this -> change_hash_assiste_data( $out_arr );
	                //注册新空间到主数据
	                $mainArr['m'][ $hash_index ] = $new_pos;
	                
	                //重新存储主数据信息到主空间
	                $new_input_main = str_pad( 0, WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . json_encode( $mainArr );
	                fseek( $this->handle, $headArr[0] );
	                fwrite( $this->handle, str_pad( $new_input_main, WECLU_HASH_BLOCK_SIZE, ' ' ), WECLU_HASH_BLOCK_SIZE );
	            }
	        } else {   //如果没有数据信息，则只有map信息
	            //分裂主信息
	            //寻找新空间地址
	            fseek( $this->handle, 0, SEEK_END );
	            $new_pos = ftell( $this->handle );
	            $remain_input = str_pad( 0, WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . substr( $main_json, $data_block_size );
	            fwrite( $this->handle, str_pad( $remain_input, WECLU_HASH_BLOCK_SIZE, ' ' ), WECLU_HASH_BLOCK_SIZE );
	            //移回指针
	            fseek( $this->handle, $headArr[0] );
	            $main_input = str_pad( base_convert( $new_pos, 10, 32 ), WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . substr( $main_json, 0, $data_block_size );
	            fwrite( $this->handle, str_pad( $main_input, WECLU_HASH_BLOCK_SIZE, ' ' ), WECLU_HASH_BLOCK_SIZE );
	        }
	    } else {
	        fseek( $this->handle, $headArr[0] );
	        $input_str = str_pad( 0, WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . $main_json;
	        fwrite( $this->handle, str_pad( $input_str, WECLU_HASH_BLOCK_SIZE, ' ' ), WECLU_HASH_BLOCK_SIZE );
	    }
	    
	    $mainArr['n'] = $headArr;
	    return true;
	}
	
	//获取数据落区
	private function get_hash_locate_area( $hash, &$mainArr ) {
	    $locate = null;
	    if ( ! empty( $hash ) && !empty( $mainArr ) && isset( $mainArr['m'] ) ) {
            foreach ( $mainArr['m'] as $hash_index => $hash_val ) {
                if ( $hash_index <= $hash ) $locate = $hash_index;
            }
	    }
	    return $locate;
	}
	
	//获取辅助空间数据
	private function get_hash_assist_data( $pos ) {
	    $this -> set_hash_assist_handle();
	    $headArr = array( $pos );
	    fseek( $this->assist_handle, $pos );
	    $str = trim( fread( $this->assist_handle, WECLU_HASH_ASSIST_BLOCK_SIZE ) );
	    
	    $str_data = trim( substr( $str, WECLU_HASH_HEAD_BLOCK_SIZE ) );
	    $head = base_convert( substr( $str, 0, WECLU_HASH_HEAD_BLOCK_SIZE ), 32, 10 );
	    while ( $head > 0 ) {
	        $headArr[] = $head;
	        fseek( $this->assist_handle, $head );
	        $str = trim( fread( $this->assist_handle, WECLU_HASH_ASSIST_BLOCK_SIZE ) );
	        if ( ! empty( $str ) ) {
	            $str_data .= trim( substr( $str, WECLU_HASH_HEAD_BLOCK_SIZE ) );
	            $head = base_convert( substr( $str, 0, WECLU_HASH_HEAD_BLOCK_SIZE ), 32, 10 );
	        }
	    }
	    $data = json_decode( $str_data, true );
	    return array( $headArr, $data );
	}
	
	//辅助空间数据存储， 此处不考虑数据分裂，只负责存储
	private function change_hash_assiste_data( &$data, $headArr=null ) {
	    $this->set_hash_assist_handle();
	    //如果headerArr为空，则新开辟hash槽
	    if ( empty( $headArr ) ) {
	        fseek( $this->assist_handle, 0, SEEK_END );
	        $headArr = array( ftell( $this->assist_handle ) );
	    }
	    //新处理新空间
	    reset( $data );
	    $hash_index = key( $data );
	    $data_json = json_encode( $data );
	    $json_len = strlen( $data_json );
	    $head_count = count( $headArr );
	    $data_block_size = WECLU_HASH_ASSIST_BLOCK_SIZE - WECLU_HASH_HEAD_BLOCK_SIZE;     //数据块真实长度
	    
	    if ( $json_len > $data_block_size ) {
	        for ( $i=0; $i < $head_count; $i++ ) {
	            //判断是否是最后一个元素
	            if ( ( $i + 1 ) == $head_count ) {
	                //仍然有剩余数据
	                if ( $json_len > $head_count * $data_block_size ) {
	                    fseek( $this->assist_handle, 0, SEEK_END );
	                    $headArr[$i+1] = ftell( $this->assist_handle );
	                    $input_str = str_pad( 0, WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . substr( $data_json, ( $i + 1 ) * $data_block_size );
	                    fwrite( $this->assist_handle, str_pad( $input_str, WECLU_HASH_ASSIST_BLOCK_SIZE, ' ' ), WECLU_HASH_ASSIST_BLOCK_SIZE );
	                } else {
	                    $headArr[$i+1] = 0;
	                }
	            }
	            fseek( $this->assist_handle, $headArr[$i] );
	            $input_str = str_pad( base_convert( $headArr[$i+1], 10, 32 ), WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . substr( $data_json, $i * $data_block_size, $data_block_size );
	            fwrite( $this->assist_handle, str_pad( $input_str, WECLU_HASH_ASSIST_BLOCK_SIZE, ' ' ), WECLU_HASH_ASSIST_BLOCK_SIZE );
	        }
	    } else {
	        fseek( $this->assist_handle, $headArr[0] );
	        $input_str = str_pad( 0, WECLU_HASH_HEAD_BLOCK_SIZE, '0', STR_PAD_LEFT ) . $data_json;
	        fwrite( $this->assist_handle, str_pad( $input_str, WECLU_HASH_ASSIST_BLOCK_SIZE, ' ' ), WECLU_HASH_ASSIST_BLOCK_SIZE );
	    }
	    return array( $hash_index, $headArr[0] );
	} 
	
}
	
	
	/***********************************************************************************
	 * B-TREE 索引
	 * 说明： btree分三部分空间，索引空间，索引数据空间，公共大数据空间
	 * zjr
	 */
class BtreeIndex {
    private $index_path = null;
    private $space = null;
    private $handle = array();
    private $node = array();
    private $header = array();
    private $key = null;
    private $keyindex = null;
    public function __construct( $space ) {
        $this -> space = $space;
    }
    
    public function __destruct() {
        $this -> close();
    }
    
    /**
     * 关闭文件存储引擎 并解锁
     * @param handle  文件句柄
     */
    private function close() {
        if ( ! empty( $this -> handle ) ) {
            foreach ( $this -> handle as $key => $handle ) {
                fclose( $this -> handle[$key] );
                unset( $this -> handle[$key] );
            }
        }
    }
    
    /**
     * 切换表
     * @param unknown $table
     */
    public function select_table( $table ) {
        $this -> close();
        $this -> keyindex = null;
        $this -> index_path = WECLU_BASE_PATH . 'index/' . $this -> space . '/' . $table . '/';
        if ( ! is_dir( $this->index_path ) ) weclu_mkdirs( $this->index_path );
    }
    
	/**
	 * 设置btree索引句柄
	 */
	private function set_btree_handle( $key ) {
	    if ( $this->keyindex != $key ) {
	        if ( empty( $this -> handle[$key] ) ) {
	            //关闭当前数据句柄
	            $btree_path = $this->index_path . $key . '.index';
	            if ( is_file( $btree_path ) ) {
	                $this -> handle[$key] = fopen( $btree_path, 'r+' );
	            } else {
	                throw new Exception("no this search index '{$key}', please try to create index!");
	            }
	        }
	        $this->keyindex = $key;
	    }
	}
	
	/**
	 * 初始化头部
	 */
	private function init_btree_head() {
	    fseek( $this->handle[$this->keyindex], 0 );
	    $header = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_HEADER_BLOCK_SIZE ) );
	    $headerArr = json_decode( $header, true );
	    $this->header = ! empty( $headerArr ) ? $headerArr : array();
	    if ( empty( $this->header ) ) {
	        $this->header = array( $this->keyindex => base_convert( WECLU_BTREE_HEADER_BLOCK_SIZE, 10, 32) );
	        fseek( $this->handle[$this->keyindex], 0 );
	        fwrite( $this->handle[$this->keyindex], str_pad( json_encode( $this->header ), WECLU_BTREE_HEADER_BLOCK_SIZE, ' ' ), WECLU_BTREE_HEADER_BLOCK_SIZE );
	    }
	}
	
	private function set_bree_head( $key, $pos=0 ) {
	    if ( empty( $pos ) ) {
	        if ( ! isset( $this->header[$key] ) ) {
	            fseek( $this->handle[$key], 0, SEEK_END );
	            $pos = base_convert( ftell( $this->handle[$key] ), 10, 32 );
	        } else {
	            $pos = $this->header[$key];
	        }
	    }
	    $this->header[$key] = $pos;
	    fseek( $this->handle[$key], 0 );
	    fwrite( $this->handle[$key], str_pad( json_encode( $this->header ), WECLU_BTREE_HEADER_BLOCK_SIZE, ' ' ), WECLU_BTREE_HEADER_BLOCK_SIZE );
	}
	
	private function init_index( $key, $val ) {
	    $this->key = $val;
	    $this->node = array();
	    $this -> set_btree_handle( $key );
	}
	
	/**
	 * 创建索引
	 * @param array $indexArr
	 */
	public function create_index( $indexArr ) {
		$keyArr = array();
		if ( ! empty( $indexArr ) ) {
			foreach ( $indexArr as $index ) {
				$dbindex = strlen( $index ) > 32 ? md5( $index ) : $index;
				if ( ! is_file( $this -> index_path . $dbindex . '.index' ) ) {
					$keyArr[$index] = 1;
					file_put_contents( $this -> index_path . $dbindex . '.index', ' ' );
				}
			}
		}
		return $keyArr;
	}
	
	/**
	 * 删除索引
	 * @param unknown $indexArr
	 */
	public function delete_index( $indexArr ) {
		$indexs = array();
		if ( ! empty( $indexArr ) ) {
			foreach ( $indexArr as $index ) {
				$dbindex = strlen( $index ) > 32 ? md5( $index ) : $index;
				if ( is_file( $this -> index_path . $dbindex . '.index' ) ) {
					unlink( $this -> index_path . $dbindex . '.index' );
					$indexs[] = $index;
				}
			}
		}
		return $indexs;
	}
	
	/**
	 * 添加btree索引入口
	 * @param unknown $key
	 * @param unknown $pos
	 * $keyArr = array( 'name' => 'zjr' ) )   字段， 值
	 */
	public function set_index( $keyArr, $id ) {
		$retAddr = array();
		if ( ! empty( $keyArr ) && ! empty( $id ) ) {
		    foreach ( $keyArr as $key => $valArr ) {
		        foreach ( $valArr as $val) {
		            $this -> init_index( $key, $val );
		            flock( $this->handle[$key], LOCK_EX );
		            $this -> init_btree_head();
// 		            $this -> set_bree_head( $key );
		            if ( isset( $this->header[$key] ) && !empty( $this->header[$key] ) ) {
		                //获取根节点位置
		                $addr = base_convert( $this->header[$key], 32, 10 );
		                $retAddr[$key] = $this -> add_btree_node( $val, $addr, $id );
		            }
		            flock( $this->handle[$key], LOCK_UN );
		        }
		    }
		}
	    return $retAddr;
	}
	
	public function get_all_index_key() {
	    $indexArr = array();
	    $indexs = glob( $this -> index_path . '*' );
	    if ( $indexs ) {
	        foreach ( $indexs as $index_path ) {
	        	$index = basename( $index_path );
	        	$index_name = substr( $index, 0, strripos( $index, '.index' ) );
	            if ( ! empty( $index_name ) ) {
	                $indexArr[$index_name] = 1;
	            }
	        }
	    }
	    return $indexArr;
	}
	
	/**
	 * 查找
	 */
	public function get_index( $keyArrs, &$storageObj ) {
		$retAddr = array();
		if ( !isset( $keyArrs[0] ) ) $keyArrs = array( $keyArrs );
		foreach ( $keyArrs as $keyArr ) {
			$tmpAddr = array();
			foreach ( $keyArr as $key => $val ) {
			    $index_arr = array();
			    //如果key已经是id，则不用去查索引
			    if ( $key == '_id' ) {
			        if ( is_array( $val ) ) {
			            if ( ! isset( $val[0] ) || ! is_array( $val[0] ) ) $val = array( $val );
			            $index_arr = $this -> match_by_id( $val, $storageObj );
			        } else {
			            $index_arr = array( base_convert( $val, 10, 32) );
			        }
			    } else {
			        $key = strlen( $key ) > 32 ? md5( $key ) : $key;
			        $this -> init_index( $key, $val );
			        flock( $this->handle[$key], LOCK_SH );
			        $this -> init_btree_head();
			        //获取根节点位置
			        $addr = isset( $this->header[$key] ) ? $this->header[$key] : 0;
			        if ( ! is_array( $val ) ) {
			            list( $i, $addr ) = $this->get_btree_index_belong_area( $val, $addr, 0 );
			            $data = isset( $this->node[$i][$val] ) ? $this->node[$i][$val] : array();
			            $index_arr = $this -> parse_btree_node_data( $data );
			        } else {
			            if ( !isset( $val[0] ) || !is_array( $val[0] ) ) $val = array( $val );
			            $this -> get_btree_index_by_condition( $index_arr, $addr, $val );
			        }
			        flock( $this->handle[$key], LOCK_UN );
			    }
				
				if ( ! empty( $tmpAddr ) ) {
					//计算数组交集
					$tmpAddr = array_intersect( $tmpAddr, $index_arr );
				} else {
					$tmpAddr = $index_arr;
				}
				unset( $index_arr );
			}
			//并集
			$retAddr = array_merge( $retAddr, $tmpAddr );
			unset( $tmpAddr );
			//过滤重复
			$retAddr = array_unique( $retAddr );
		}
	    return $retAddr;
	}
	
	public function update_index( $key, $val, $newVal, $index ) {
		//首先查找
		$this -> init_index( $key, $val );
		flock( $this->handle[$this->keyindex], LOCK_EX );
		$this -> init_btree_head();
		$addr = isset( $this->header[$key] ) ? $this->header[$key] : 0;
		//先删除，再新增
		$this -> get_btree_index_belong_area( $val, $addr, 0 );
		$data_pos = base_convert( $index, 10, 32 );
		$this -> del_btree_node( $data_pos );
		//删除后再新增
		$this->key = $newVal;
		$this->node = array();
		//获取根节点位置
		$addr = base_convert( $this->header[$key], 32, 10 );
		$retAddr = $this -> add_btree_node( $newVal, $addr, $index );
		
		flock( $this->handle[$this->keyindex], LOCK_UN );
		return $retAddr;
	}
	
	/**
	 * 删除索引
	 */
	public function del_index( $keyArr, $id ) {
	    if ( ! empty( $keyArr ) && ! empty( $id ) ) {
	        foreach ( $keyArr as $key => $valArr ) {
	            foreach ( $valArr as $val) {
	                $this -> init_index( $key, $val );
	                flock( $this->handle[$this->keyindex], LOCK_EX );
	                $this -> init_btree_head();
	                if ( isset( $this->header[$key] ) ) {
	                    //获取根节点位置
	                    list( $i, $addr ) = $this -> get_btree_index_belong_area( $val, $this->header[$key], 0 );
	                    $id = base_convert( $id, 10, 32 );
	                    $this -> del_btree_node( $id );
	                }
	                flock( $this->handle[$this->keyindex], LOCK_UN );
	            }
	        }
	    }
	    return true;
	}
	
	/**
	 * 获取btree所属区块
	 * zjr
	 */
	private function get_btree_index_belong_area( $value, $addr, $i ) {
	    //转化成十进制地址
	    $addr = base_convert( $addr, 32, 10 );
	    fseek( $this->handle[$this->keyindex], $addr );
	    $node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	    $type = substr( $node_str, 0, 1 );
	    $node = json_decode( substr( $node_str, 1 ), true );
	    $this->node[$i] = $node;
	    if ( ! empty( $node ) ) {
	        list( $prev, $key, $next ) = $this -> seek_btree_node_key( $this->node[$i], $value );
	        //直到定位到底层才结束
	        if ( $type != '@' ) {
	            list( $i, $addr ) = $this -> get_btree_index_belong_area( $value, $node[$key], ++$i );
	        }
	    }
	    return array( $i, $addr );
	}
	
	private function get_btree_index_by_condition( &$retData, $addr, $condition ) {
	    $addr = base_convert( $addr, 32, 10 );
	    fseek( $this->handle[$this->keyindex], $addr );
	    $node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	    $type = substr( $node_str, 0, 1 );
	    $nodes = json_decode( substr( $node_str, 1 ), true );
	    if ( ! empty( $nodes ) ) {
	    	$newNodes = $this -> check_is_match_condition( $nodes, $condition, $type );
	    	if ( $type == '@' ) {
	    		foreach ( $newNodes as $key => $addr ) {
	    			$retData = array_merge( $retData, $this -> parse_btree_node_data( $addr ) );
	    		}
	    	} else {
	    		foreach ( $newNodes as $key => $addr ) {
	    			$this -> get_btree_index_by_condition( $retData, $addr, $condition );
	    		}
	    	}
	    }
	}
	
	/**
	 * 解析节点数据
	 */
	private function parse_btree_node_data( $data ) {
	    $index_arr = array();
	    if ( reset( $data ) == 1 ) {
	        $pos = key( $data );
	        //获取所有的数据
	        $pos_arr = array();
	        $dataStr = $this -> get_btree_data_node( $pos, $pos_arr );
	        if ( ! empty( $dataStr ) ) {
	            $index_arr = explode( ',', $dataStr );
	        }
	    } else {
	        $index_arr = array_keys( $data );
	    }
	    return $index_arr;
	}
	
	private function add_btree_node( $value, $addr, $data_pos ) {
	    $node = array();
	    //获取根节点信息
	    if ( ! empty( $addr ) ) {
	        fseek( $this->handle[$this->keyindex], $addr );
	        $node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	        $type = substr( $node_str, 0, 1 );
	        $node = json_decode( substr( $node_str, 1 ), true );
	    } else {
	        fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	        $addr = ftell( $this->handle[$this->keyindex] );
	    }
	    $data_pos = base_convert( $data_pos, 10, 32 );
	    $storeKey = $value;
	    if ( ! empty( $node ) ) {
	        $this->node[0] = $node;
	        //根节点是叶子节点
	        if ( $type == '@' ) {
	            $data_index = isset( $this->node[0][$storeKey] ) ? $this->node[0][$storeKey] : array();
	            $data_index[$data_pos] = 0;
	        	$this->node[0][$storeKey] = $this -> handle_btree_data_node( $data_index );
	        	$this -> foreach_btree_add_init_node();
	        } else {
	        	list( $prev, $key, $next ) = $this -> seek_btree_node_key( $this->node[0], $value );
	        	if ( $key >= $value ) {
        			list( $i, $addr ) = $this -> get_btree_index_belong_area( $value, $node[$key], 1 );
        			$data_index = isset( $this->node[$i][$storeKey] ) ? $this->node[$i][$storeKey] : array();
        			$data_index[$data_pos] = 0;
        			$this->node[$i][$storeKey] = $this -> handle_btree_data_node( $data_index );
        			$this -> foreach_btree_add_init_node();
	        	} else {
	        		//索引树中还没有出现的大数
        			list( $i, $addr ) = $this -> get_btree_index_belong_area( $value, $node[$key], 1 );
        			$data_index = isset( $this->node[$i][$storeKey] ) ? $this->node[$i][$storeKey] : array();
        			$data_index[$data_pos] = 0;
        			$this->node[$i][$storeKey] = $this -> handle_btree_data_node( $data_index );
        			$this -> insert_btree_node_max_key();
	        	}
	        }
	    } else {
	        $this->node[0][$storeKey] = array( $data_pos => 0 );
	        $this -> foreach_btree_add_init_node();
	    }
	    
	    return $addr;
	}
	
	/**
	 * 插入btree 叶子节点
	 */
	private function foreach_btree_add_init_node( $i=null ) {
		$lays = count( $this->node );
		if ( $i < 0 || $i > $lays + 1 ) return false;
		if ( $i === null ) $i = $lays - 1;
		ksort( $this->node[$i] );
		if ( $i - 1 < 0 ) $addr = base_convert( $this->header[$this->keyindex], 32, 10 );
		else {
		    //通过当前节点最后一个元素，寻找父节点，从而确定该节点的位置
		    end($this->node[$i]);
		    $endKey = key( $this->node[$i] );
		    list( $prevKey, $curKey, $nexKey ) = $this -> seek_btree_node_key( $this->node[$i-1], $endKey );
		    $addr = base_convert( $this->node[$i-1][$curKey], 32, 10 );
		}
	    //叶子节点
	    if ( $lays == $i + 1 )
	    	$type = '@';
	    else $type = '0';
	    //先判断node是否超出了块长度
	    $node = $this->node[$i];
	    $jsonNode = json_encode( $node );
	    $nodeLen = strlen( $jsonNode );
	    //超出长度就要进行数据分裂
	    if ( $nodeLen > WECLU_BTREE_BLOCK_SIZE - 1 ) {
	    	//判断是否到达根节点
	    	if ( $i == 0 ) {
	    		//如果node中有两个以上的关键字 则数据一分为2
	    		$count = count( $node );
	    		$node_right = array_splice( $node, ceil( $count/2 ) );
	    		//更新左节点
	    		$leftAddr = $addr;
	    		$jsonLeft = json_encode( $node );
	    		$leftMaxVal = end( $node );
	    		$leftMaxKey = key( $node );
	    		fseek( $this->handle[$this->keyindex], $addr );
	    		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonLeft, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    		//新增右节点存储
	    		fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	    		$rightAddr = ftell( $this->handle[$this->keyindex] );
	    		$jsonRight = json_encode( $node_right );
	    		$rightMaxVal = end( $node_right );
	    		$rightMaxKey = key( $node_right );
	    		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonRight, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    		//上移根节点
	    		$root = array( $leftMaxKey => base_convert( $leftAddr, 10, 32 ), $rightMaxKey => base_convert( $rightAddr, 10, 32 ) );
	    		$jsonRoot = json_encode( $root );
	    		fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	    		$rootAddr = ftell( $this->handle[$this->keyindex] );
	    		fwrite( $this->handle[$this->keyindex], '0' . str_pad( $jsonRoot, WECLU_BTREE_BLOCK_SIZE, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    		//修改头部根节点位置
	    		$this->set_bree_head( $this->keyindex, base_convert( $rootAddr, 10, 32 ) );
	    	} else {
	    		//判断左右兄弟节点是否丰满，先左后右
	    		//先判断有没有兄弟节点未丰满
	    		//先检测左节点
	    		if ( ! empty( $prevKey ) ) {
	    			$leftNodeAddr = base_convert( $this->node[$i-1][$prevKey], 32, 10 );
	    			if ( ! empty( $leftNodeAddr ) ) {
	    				fseek( $this->handle[$this->keyindex], $leftNodeAddr );
	    				$left_node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	    				if ( strlen( $left_node_str ) <= WECLU_BTREE_BLOCK_SIZE / 3 ) {
	    					//移出数据 floor( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) )
	    					$outNums = floor( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) );
	    					$leftNode = json_decode( substr( $left_node_str, 1 ), true );
	    					$currentNode = array_splice( $node, $outNums );
	    					$leftNode = array_merge( $leftNode, $node );
	    					//保存左节点
	    					fseek( $this->handle[$this->keyindex], $leftNodeAddr );
	    					fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $leftNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    					//保存当前节点
	    					fseek( $this->handle[$this->keyindex], $addr );
	    					fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $currentNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    					//修改父节点
	    					end($leftNode);
	    					$leftEndKey = key($leftNode);
	    					$this->node[$i-1][$leftEndKey] = $this->node[$i-1][$prevKey];
	    					unset( $this->node[$i-1][$prevKey] );
	    					//判断父节点
	    					return $this -> foreach_btree_add_init_node( --$i );
	    				}
	    			}
	    		}
	    		//左节点丰满或不存在时再检测右节点
	    		if ( ! empty( $nexKey ) ) {
	    			$rightNodeAddr = base_convert( $this->node[$i-1][$nexKey], 32, 10 );
	    			if ( ! empty( $rightNodeAddr ) ) {
	    				fseek( $this->handle[$this->keyindex], $rightNodeAddr );
	    				$right_node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	    				if ( strlen( $right_node_str ) <= WECLU_BTREE_BLOCK_SIZE / 3 ) {
	    					//数据右端移出数据 floor( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) )
	    					$outNums = floor( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) );
	    					$rightNode = json_decode( substr( $right_node_str, 1 ), true );
	    					$outNode = array_splice( $node, $outNums * 3 );
	    					$rightNode = array_merge( $outNode, $rightNode );
	    					//保存右节点
	    					fseek( $this->handle[$this->keyindex], $rightNodeAddr );
	    					fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $rightNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    					//保存当前节点
	    					fseek( $this->handle[$this->keyindex], $addr );
	    					fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $node ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    					//修改父节点
	    					end($node);
	    					$leftEndKey = key($node);
	    					$this->node[$i-1][$leftEndKey] = $this->node[$i-1][$curKey];
	    					unset( $this->node[$i-1][$curKey] );
	    					//判断父节点
	    					return $this -> foreach_btree_add_init_node( --$i );
	    				}
	    			}
	    		}
	    		//如果左右节点都已经丰满，就要进行数据分裂, 一分为二时，右节点位置不用变， 左节点新增
	    		$count = count( $node );
	    		$node_right = array_splice( $node, ceil( $count/2 ) );
	    		//新增左节点存储
	    		fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	    		$leftAddr = ftell( $this->handle[$this->keyindex] );
	    		$jsonLeft = json_encode( $node );
	    		$leftMaxVal = end( $node );
	    		$leftMaxKey = key( $node );
	    		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonLeft, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    		//更新右节点
	    		$jsonRight = json_encode( $node_right );
	    		fseek( $this->handle[$this->keyindex], $addr );
	    		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonRight, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    		//更新父节点
	    		$this->node[$i-1][$leftMaxKey] = base_convert( $leftAddr, 10, 32 );
	    		return $this -> foreach_btree_add_init_node( --$i );
	    	}
	    } else {
	        fseek( $this->handle[$this->keyindex], $addr );
	    	fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonNode, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    }
	    return true;
	}
	
	private function seek_btree_node_key( $nodes, $key ) {
	    $prev = $cur = $next = '';
        foreach ( $nodes as $k => $p ) {
            if( $cur >= $key && $k > $key ) {
                $next = $k;
                break;
            }
            $prev = $cur;
            $cur = $k;
        }
	    return array( $prev, $cur, $next );
	}
	
	/**
	 * 新增最大节点时，遵循原则， 长度不够存，向右分裂，否则只修改父节点最大值，依次迭代
	 * @param unknown $i
	 * @return boolean
	 */
	private function insert_btree_node_max_key( $i=null ) {
	    $lays = count( $this->node );
	    if ( $i < 0 || $i > $lays + 1 ) return false;
	    if ( $i === null ) $i = $lays - 1;
	    ksort( $this->node[$i] );
	    if ( $i - 1 < 0 ) $addr = base_convert( $this->header[$this->keyindex], 32, 10 );
	    else {
	        //通过当前节点最后一个元素，寻找父节点，从而确定该节点的位置
	        $addr = base_convert( end( $this->node[$i-1] ), 32, 10 );
	        $endKey = key( $this->node[$i-1] );
	    }
	    //叶子节点
	    if ( $lays == $i + 1 )
	        $type = '@';
	    else $type = '0';
	    //先判断node是否超出了块长度
	    $node = $this->node[$i];
	    $jsonNode = json_encode( $node );
	    $nodeLen = strlen( $jsonNode );
	    //超出长度就要进行数据分裂
	    if ( $nodeLen > WECLU_BTREE_BLOCK_SIZE - 1 ) {
	        //判断是否到达根节点
	        if ( $i == 0 ) {
	            //如果node中有两个以上的关键字 则数据一分为2
	            $count = count( $node );
	            $node_right = array_splice( $node, ceil( $count/2 ) );
	            //更新左节点
	            $leftAddr = $addr;
	            $jsonLeft = json_encode( $node );
	            $leftMaxVal = end( $node );
	            $leftMaxKey = key( $node );
	            fseek( $this->handle[$this->keyindex], $addr );
	            fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonLeft, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	            //新增右节点存储
	            fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	            $rightAddr = ftell( $this->handle[$this->keyindex] );
	            $jsonRight = json_encode( $node_right );
	            $rightMaxVal = end( $node_right );
	            $rightMaxKey = key( $node_right );
	            fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonRight, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	            //上移根节点
	            $root = array( $leftMaxKey => base_convert( $leftAddr, 10, 32 ), $rightMaxKey => base_convert( $rightAddr, 10, 32 ) );
	            $jsonRoot = json_encode( $root );
	            fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	            $rootAddr = ftell( $this->handle[$this->keyindex] );
	            fwrite( $this->handle[$this->keyindex], '0' . str_pad( $jsonRoot, WECLU_BTREE_BLOCK_SIZE, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	            //修改头部根节点位置
	            $this->set_bree_head( $this->keyindex, base_convert( $rootAddr, 10, 32 ) );
	        } else {
	            //不用判断兄弟节点是否丰满， 直接分裂， 向右增长
	            $count = count( $node );
	    		$node_right = array_splice( $node, ceil( $count/2 ) );
	    		//更新左节点
	    		$leftAddr = $addr;
	    		$jsonLeft = json_encode( $node );
	    		$leftMaxVal = end( $node );
	    		$leftMaxKey = key( $node );
	    		fseek( $this->handle[$this->keyindex], $addr );
	    		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonLeft, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    		//新增右节点存储
	    		fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	    		$rightAddr = ftell( $this->handle[$this->keyindex] );
	    		$jsonRight = json_encode( $node_right );
	    		$rightMaxVal = end( $node_right );
	    		$rightMaxKey = key( $node_right );
	    		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonRight, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	            //更新父节点
	            $this->node[$i-1][$leftMaxKey] = $this->node[$i-1][$endKey];
	            $this->node[$i-1][$rightMaxKey] = base_convert( $rightAddr, 10, 32 );
	            unset( $this->node[$i-1][$endKey] );
	            return $this -> foreach_btree_add_init_node( --$i );
	        }
	    } else {
	        fseek( $this->handle[$this->keyindex], $addr );
	        fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonNode, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	        //修改父节点
	        if ( $i > 0 ) {
	            end( $node );
	            $maxKey = key( $node );
	            $this->node[$i-1][$maxKey] = $this->node[$i-1][$endKey];
	            unset( $this->node[$i-1][$endKey] );
	            $this -> insert_btree_node_max_key( --$i );
	        }
	    }
	    return true;
	}
	
/**
	 * 插入btree 叶子节点
	 */
	private function del_btree_node( $data_pos, $i=null ) {
		$lays = count( $this->node );
		if ( $i < 0 || $i > $lays + 1 ) return false;
		if ( $i === null ) $i = $lays - 1;
		if ( $i - 1 < 0 ) $addr = base_convert( $this->header[$this->keyindex], 32, 10 );
		else {
		    //通过当前节点最后一个元素，寻找父节点，从而确定该节点的位置
		    end($this->node[$i]);
		    $endKey = key( $this->node[$i] );
		    list( $prevKey, $curKey, $nexKey ) = $this -> seek_btree_node_key( $this->node[$i-1], $endKey );
		    $addr = base_convert( $this->node[$i-1][$curKey], 32, 10 );
		}
		//先判断节点块中是否存在需要删除的节点
		//叶子节点
		if ( $lays == $i + 1 ) {
		    $type = '@';
		    if ( isset( $this->node[$i][$this->key] ) ) {
		        $data_index = $this -> del_btree_data_node( $this->node[$i][$this->key], $data_pos );
		        if ( empty( $data_index ) ) {
		            unset( $this->node[$i][$this->key] );
		        }
		    }
		} else {
		    $type = '0';
		    unset( $this->node[$i][$this->key] );
		}
		//删除后判断是否要进行合并
		$jsonNode = json_encode( $this->node[$i] );
		$nodeLen = strlen( $jsonNode );
	    //如果小于1/4 就要进行合并
	    if ( $nodeLen <= WECLU_BTREE_BLOCK_SIZE / 4 ) {
	    	//判断左边是否丰满
	    	if ( ! empty( $prevKey ) ) {
	    		$leftNodeAddr = base_convert( $this->node[$i-1][$prevKey], 32, 10 );
	    		if ( ! empty( $leftNodeAddr ) ) {
	    			fseek( $this->handle[$this->keyindex], $leftNodeAddr );
	    			$left_node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	    			if ( strlen( $left_node_str ) >= ( WECLU_BTREE_BLOCK_SIZE * 3 ) / 4 ) {
	    				$outNums = ceil( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) );
	    	            $leftNode = json_decode( substr( $left_node_str, 1 ), true );
	    	            $outNode = array_splice( $leftNode, $outNums * 3 );
	    	            $this->node[$i] = array_merge( $outNode, $this->node[$i] );
	    	            //保存左节点
	    	            fseek( $this->handle[$this->keyindex], $leftNodeAddr );
	    	            fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $leftNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    	            //保存当前节点
	    	            fseek( $this->handle[$this->keyindex], $addr );
	    	            fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $this->node[$i] ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    	            //修改父节点
	    	            end($leftNode);
	    	            $leftEndKey = key($leftNode);
	    	            $this->node[$i-1][$leftEndKey] = $this->node[$i-1][$prevKey];
	    	            unset( $this->node[$i-1][$prevKey] );
	    	            //如果删除的是最后一个节点
	    	            if ( $endKey == $this->key ) {
	    	                //前移父节点索引
	    	                end($this->node[$i]);
	                        $endKey = key( $this->node[$i] );
	                        $this->node[$i-1][$endKey] = $this->node[$i-1][$curKey];
	    	            }
	    	            ksort( $this->node[$i-1] );
	    				//判断父节点
	    				return $this -> del_btree_node( $data_pos, --$i );
	    			}
	    		}
	    	}
	    	//左节点丰满或不存在时再检测右节点
	    	if ( ! empty( $nexKey ) ) {
	    	    $rightNodeAddr = base_convert( $this->node[$i-1][$nexKey], 32, 10 );
	    	    if ( ! empty( $rightNodeAddr ) ) {
	    	        fseek( $this->handle[$this->keyindex], $rightNodeAddr );
	    	        $right_node_str = trim( fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE ) );
	    	        if ( strlen( $right_node_str ) >= ( WECLU_BTREE_BLOCK_SIZE * 3 ) / 4 ) {
	    	            //数据右端移出数据 floor( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) )
	    	            $outNums = floor( WECLU_BTREE_BLOCK_SIZE / ( WECLU_BTREE_MAX_KEY_SIZE * 4 ) );
	    	            $rightNode = json_decode( substr( $right_node_str, 1 ), true );
	    	            $newRightNode = array_splice( $rightNode, $outNums );
	    	            $this->node[$i] = array_merge( $this->node[$i], $rightNode );
	    	            //保存右节点
	    	            fseek( $this->handle[$this->keyindex], $rightNodeAddr );
	    	            fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $newRightNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    	            //保存当前节点
	    	            fseek( $this->handle[$this->keyindex], $addr );
	    	            fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $this->node[$i] ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    	            //修改父节点
	    	            end( $this->node[$i] );
	    	            $leftEndKey = key( $this->node[$i] );
	    	            $this->node[$i-1][$leftEndKey] = $this->node[$i-1][$curKey];
	    	            unset( $this->node[$i-1][$curKey] );
	    	            ksort( $this->node[$i-1] );
	    	            //判断父节点
	    	            return $this -> del_btree_node( $data_pos, --$i );
	    	        }
	    	    }
	    	}
	        //开始合并
	    	if ( ! empty( $prevKey ) ) {
    			if ( strlen( $left_node_str ) < ( WECLU_BTREE_BLOCK_SIZE * 3 ) / 4 ) {
    				$leftNode = json_decode( substr( $left_node_str, 1 ), true );
    				$mergeNode = array_merge( $leftNode, $this->node[$i] );
    				//保存左节点
    				fseek( $this->handle[$this->keyindex], $addr );
    				fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $mergeNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
    				//修改父节点
    				unset( $this->node[$i-1][$prevKey] );
    				//如果删除的是最后一个节点
    				if ( $endKey == $this->key ) {
    				    //前移父节点索引
    				    end($this->node[$i]);
    				    $endKey = key( $this->node[$i] );
    				    $this->node[$i-1][$endKey] = $this->node[$i-1][$curKey];
    				    ksort( $this->node[$i-1] );
    				}
    				//判断父节点
    				return $this -> del_btree_node( $data_pos, --$i );
    			}
	    	}
	    	//左节点丰满或不存在时再检测右节点
	    	if ( ! empty( $nexKey ) ) {
    	        if ( strlen( $right_node_str ) < ( WECLU_BTREE_BLOCK_SIZE * 3 ) / 4 ) {
    	            $rightNode = json_decode( substr( $right_node_str, 1 ), true );
    	            $mergeNode = array_merge( $this->node[$i], $rightNode );
    	            //保存右节点
    	            fseek( $this->handle[$this->keyindex], $rightNodeAddr );
    	            fwrite( $this->handle[$this->keyindex], $type . str_pad( json_encode( $mergeNode ), WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
    	            //修改父节点
    	            unset( $this->node[$i-1][$curKey] );
    	            //判断父节点
    	            return $this -> del_btree_node( $data_pos, --$i );
    	        }
	    	}
	    }
	    
	    //如果到了根节点块， 判断节点数是否大于1
	    if ( $i == 0 && $lays > 1 ) {
	        if ( count( $this->node[$i] ) < 2 ) {
	            //修改头部根节点位置
	            $this->set_bree_head( $this->keyindex, end( $this->node[$i] ) );
	        }
	    }
		fseek( $this->handle[$this->keyindex], $addr );
		fwrite( $this->handle[$this->keyindex], $type . str_pad( $jsonNode, WECLU_BTREE_BLOCK_SIZE - 1, ' ' ), WECLU_BTREE_BLOCK_SIZE );
	    return true;
	}
	

	/**
	 * 处理数据节点
	 */
	private function handle_btree_data_node( $data_index ) {
	    $data = array();
	    if ( ! empty( $data_index ) ) {
	        //如果节点超过了5个
	        $count = count( $data_index );
	        if ( $count >= 5 ) {
	            $posArr = $this -> insert_btree_data_node( array_keys( $data_index ) );
	            $data = array( base_convert( $posArr[0], 10, 32) => 1 );
	        } else {
	            //判断是否有独立的数据空间
	            if ( reset( $data_index ) == 1 ) {
	                $pos = key( $data_index );
	                $index_arr = array();
	                //获取所有的独立数据
	                $pos_arr = array();
	                $dataStr = $this -> get_btree_data_node( $pos, $pos_arr );
	                if ( ! empty( $dataStr ) ) {
	                    $index_arr = explode( ',', $dataStr );
	                }
	                foreach ( $data_index as $index => $val ) {
	                    if ( $index != $pos ) {
	                        $index_arr[] = $index;
	                    }
	                }
	                //重置所有的独立数据
	                $this -> insert_btree_data_node( $index_arr, $pos_arr );
	                $data = array( $pos => 1 );
	            } else {
	                $data = $data_index;
	            }
	        }
	    }
	    return $data;
	}
	
	private function insert_btree_data_node( $data_index, $pos_arr=array() ) {
	    if( ! empty( $data_index ) ) {
	        //过滤重复项
	        $data_index = array_unique( $data_index );
	        $dataStr = implode( ',', $data_index );
	        //判断需要多少个空间
	        $dataLen = strlen( $dataStr );
	        $data_block_size = WECLU_BTREE_BLOCK_SIZE - WECLU_BTREE_POS_BLOCK_SIZE;
	        $space_need_nums = ceil( $dataLen / $data_block_size );
	        $space_has_nums = count( $pos_arr );
	        if ( $space_has_nums < $space_need_nums ) {
	            $need_new_nums = $space_need_nums - $space_has_nums;
	            $new_pos_arr = array();
	            for ( $i = 0; $i < $need_new_nums; $i++ ) {
	                if ( $i == 0 ) {
	                    fseek( $this->handle[$this->keyindex], 0, SEEK_END );
	                    $new_pos_arr[$i] = ftell( $this->handle[$this->keyindex] );
	                } else {
	                    $new_pos_arr[$i] = $new_pos_arr[$i-1] + WECLU_BTREE_BLOCK_SIZE;
	                }
	            }
	            $pos_arr = array_merge( $pos_arr, $new_pos_arr );
	        }
	        if ( ! empty( $pos_arr ) ) {
	            for ( $j = 0; $j < $space_need_nums; $j++ ) {
	                fseek( $this->handle[$this->keyindex], $pos_arr[$j] );
	                $nextPos32 = ( $j+1 == $space_need_nums ) ? '0' : base_convert( $pos_arr[$j+1], 10, 32 );
	                $nextPos = str_pad( $nextPos32, WECLU_BTREE_POS_BLOCK_SIZE, '0', STR_PAD_LEFT);
	                $data = substr( $dataStr, $j * $data_block_size, $data_block_size );
	                $data = str_pad( $data, $data_block_size, ' ' );
	                fwrite( $this->handle[$this->keyindex], $nextPos . $data, WECLU_BTREE_BLOCK_SIZE );
	            }
	        }
	    }
	    return $pos_arr;
	}
	
	private function get_btree_data_node( $pos, &$pos_arr ) {
	    $dataStr = '';
	    $pos = ! empty( $pos ) ? base_convert( $pos, 32, 10 ) : 0;
	    if ( ! empty( $pos ) ) {
	        $pos_arr[] = $pos;
	        fseek( $this->handle[$this->keyindex], $pos );
	        $str = fread( $this->handle[$this->keyindex], WECLU_BTREE_BLOCK_SIZE );
	        $nextPos = substr( $str, 0, WECLU_BTREE_POS_BLOCK_SIZE );
	        $dataStr = trim( substr( $str, WECLU_BTREE_POS_BLOCK_SIZE ) );
	        $dataStr .= $this -> get_btree_data_node( $nextPos, $pos_arr );
	    }
	    return $dataStr;
	}
	
	/**
	 * 删除数据节点
	 */
	private function del_btree_data_node( $data_index, $index ) {
		if ( reset( $data_index ) == 1 ) {
			$pos = key( $data_index );
			$index_arr = array();
			$pos_arr = array();
			$dataStr = $this -> get_btree_data_node( $pos, $pos_arr );
			if ( ! empty( $dataStr ) ) {
				$index_arr = explode( ',', $dataStr );
			}
			$index_arr = array_merge( array_diff( $index_arr, array( $index ) ) );
			if ( ! empty( $index_arr ) ) {
				$this -> insert_btree_data_node( $index_arr, $pos_arr );
			} else {
				$data_index = array();
			}
		} else {
			if ( isset( $data_index[$index] ) ) unset( $data_index[$index] );
		}
		return $data_index;
	}
	
	/**
	 * 判断是否符合条件的叶子节点
	 */
	private function check_is_match_condition( $nodes, $conditions, $type ) {
		$conData	= array();	//符合条件的数据
		foreach ( $nodes as $key => $node ) {
			$flag = true;
			if ( $type == '@' ) {
				foreach ( $conditions as $condition ) {
					$conBorder = array();	//条件的临界数组
					if ( isset( $condition[0] ) ) $condition = array( 'in' => $condition );
					foreach ( $condition as $con => $val ) {
						if ( isset( $conBorder[$con] ) ) {
							$flag = false;
							break;
						}
						switch ( $con ) {
							case 'like' :
								$mode = '/^'.str_replace('%', '.*', $val ).'$/i';
								if ( ! preg_match( $mode, $key ) ) $flag = false;
								else $flag = true;
								break;
							case 'in' :
								if ( ! in_array( $key, $val ) ) $flag = false;
								else $flag = true;
								break;
							case 'nin' :
								if ( in_array( $key, $val) ) $flag = false;
								else $flag = true;
								break;
							default:
								$compare = "'".$key."'".$con."'".$val."'";
								eval( "\$compareRes=$compare;" );
								if ( ! $compareRes ) $flag = false;
								else $flag = true;
						}
						if ( ! $flag ) break;
					}
					if ( $flag ) break;
				}
			} else {
				foreach ( $conditions as $condition ) {
					$conBorder = array();	//条件的临界数组
					foreach ( $condition as $con => $val ) {
						if ( isset( $conBorder[$con] ) ) {
							$flag = false;
							break;
						}
						switch ( $con ) {
							case 'like' :
								$val = str_replace('%', '', $val );
								if ( $key < $val ) $flag = false;
								else $flag = true;
								break;
							case 'in' :
								sort( $val );
								if ( $key < reset( $val ) ) $flag = false;
								elseif( $key > end( $val ) ) {
									$conBorder[$con] = 1;
									$flag = true;
								} else $flag = true;
								break;
							case '<' :
							case '<=' :
								if ( $key > $val ) $conBorder[$con] = 1;
								$flag = true;
								break;
							case '==' :
								if ( $key >= $val ) {
									$conBorder[$con] = 1;
									$flag = true;
								} else $flag = false;
								break;
							case '>' :
							case '>=' :
								if ( $key < $val ) $flag = false;
								else $flag = true;
								break;
						}
						if ( ! $flag ) break;
					}
					if ( $flag ) break;
				}
			}
			if ( $flag ) $conData[$key] = $node;
		} 
	    return $conData;
	}
	
	/**
	 * 通过条件获取值
	 * @param unknown $conditions
	 * @param unknown $storageObj
	 * @return multitype:
	 */
	private function match_by_id( $conditions, &$storageObj ) {
	    $retIds = array();
	    foreach ( $conditions as $condition ) {
	        $index_arr = array();
	        if ( isset( $condition[0] ) ) $condition = array( 'in' => $condition );
	        if ( isset( $condition['>'] ) ) $start = $condition['>']+1;
	        if ( isset( $condition['>='] ) ) $start = $condition['>='];
	        if ( isset( $condition['<'] ) ) $end = $condition['<'] - 1;
	        if ( isset( $condition['<='] ) )$end = $condition['<='];
	        if ( isset( $condition['=='] ) ) $index_arr = array( base_convert( $condition['=='], 10, 32) );
	        if ( isset( $condition['in'] ) ) {
	            $has_intersect = false;
	            foreach ( $condition['in'] as $id ) {
	                if ( ! isset( $condition['=='] ) || $condition['=='] == $id ) {
	                   $index_arr[] = base_convert( $id, 10, 32 );
	                   $has_intersect = true;
	                }
	            }
	            if ( ! $has_intersect ) continue;
	        }
	        if ( isset( $start ) || isset( $end ) ) {
	            $start = $start ? $start : 1;
	            $tableInfo = $storageObj->get_header();
	            $end = !isset($end) || $end > $tableInfo['n_id'] ? $tableInfo['n_id'] : $end;
	            $ids = $storageObj->fetch_ids( $start, $end );
                if ( ! empty( $index_arr ) ) {
                    //计算数组交集
                    $index_arr = array_intersect( $index_arr, $ids );
                } else {
                    $index_arr = $ids;
                }
	        }
	        $retIds = array_merge( $retIds, $index_arr );
	    }
	    if ( ! empty( $retIds ) ) $retIds = array_unique( $retIds );
	    return $retIds;
	}
	
}
