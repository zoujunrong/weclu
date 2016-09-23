<?php
/**
 * @description 文件存储引擎
 * @author zjr
 * @2016/07/28
 * 说明： 文件存储管理，包括空间分配
 */
class Storage {
    
	private $table = null;
	private $base_path = null;
	
	private $byte_handle = null;
	private $main_handle = null;
	private $data_handle = null;
	
	private $main_header = null;
	private $data_area = 0;		//当前数据区域
	
	public function __construct( $db_space ) {
		if ( ! empty( $db_space )) {
		    $this -> base_path = WECLU_BASE_PATH . 'data/' . $db_space . '/';
		    if ( ! is_dir( $this -> base_path ) ) weclu_mkdirs( $this -> base_path );
		}
	}
	
	public function __destruct() {
	    $this -> close();
	}
	
	public function delete_space() {
	    //关闭文件句柄
	    $this -> close();
	    return weclu_deldir( $this -> base_path );
	}
	
	public function delete_table( $table ) {
	    $table_paths = glob( $this -> base_path . $table.'@*' );
	    if ( ! empty( $table_paths ) ) {
	        foreach ( $table_paths as $path ) {
	            if ( is_file( $path ) ) unlink( $path );
	        }
	        return true;
	    }
	    return false;
	}
	
	public function create_table( $table, $table_type = 'RDS', $start_id=1 ) {
	    $main_path = $this->base_path . $table . '@main.data';
	    if ( ! is_file( $main_path ) ) {
	    	$this -> table = $table;
	    	$this -> close();   //关闭之前的数据句柄
	    	$this -> main_handle = fopen( $main_path, 'w+' );
	    	$this -> init_main_header( $table_type, $start_id );     //初始化头部信息
	    	return true;
	    }
	    return false;
	}
	
	public function select_table( $table ) {
		if ( $table != $this -> table ) {
			$this -> table = $table;
			$this -> close();  //关闭之前的数据句柄
			if ( $this -> set_main_handle() ) {
				$this -> get_main_header();
			}
		}
	}
	
	public function get_header() {
	    return $this -> main_header;
	}
	
	
	/**
	 * 设置位图句柄
	 */
	public function set_byte_handle() {
		$byte_path = $this->base_path . $this->table . '@byte.map';
		//关闭当前数据句柄
		if ( ! empty( $this -> byte_handle ) ) return true;
		if ( is_file( $byte_path ) ) {
		    $this -> byte_handle = fopen( $byte_path, 'r+' );
		} else {
		    $this -> byte_handle = fopen( $byte_path, 'w+' );
		}
		return true;
	}
	
	/**
	 * 设置主表句柄
	 */
	public function set_main_handle() {
		$main_path = $this->base_path . $this->table . '@main.data';
		//关闭当前数据句柄
		if ( ! empty( $this -> main_handle ) ) return true;
		if ( is_file( $main_path ) ) {
			$this -> main_handle = fopen( $main_path, 'r+' );
		} else {
		    throw new Exception("no exist table '{$this->table}', please try create!");
		    return false;
		}
		return true;
	}
	
	/**
	 * 设置数据句柄
	 */
	public function set_data_handle( $area ) {
	    if ( $area != $this->data_area && $area > 0 ) {
	        //关闭当前数据句柄
	        if ( ! empty( $this->data_handle ) ) fclose( $this->data_handle );
	        //开启新区域数据句柄
	        $data_path = $this->base_path . $this->table . '@' . $area . '.data';
	        if ( is_file( $data_path ) ) {
	            $this->data_handle = fopen( $data_path, 'r+' );
	        } else {
	            $this->data_handle = fopen( $data_path, 'w+' );
	        }
	        $this->data_area = $area;
	    }
	}
	
	/**
	 * 初始化主表头部信息
	 */
	public function init_main_header( $table_type, $start_id=1 ) {
	    $this->main_header = array(
	        's_id'  => $start_id,       //起始ID
            'n_id'  => $start_id > 0 ? ($start_id-1) : 0,       //最新ID
            'type'  => $table_type,   //类型
	    );
	    $this -> set_main_header();
	}
	
	/**
	 * 获取主表头部信息
	 */
	public function get_main_header() {
	    fseek( $this->main_handle, 0 );
	    $header = trim( fread( $this->main_handle, WECLU_HEADER_BLOCK_SIZE ) );
	    $this->main_header = unserialize( $header );
	}
	
	/**
	 * 设置主表头部信息
	 */
	public function set_main_header() {
	    if ( ! empty( $this->main_header ) ) {
	        fseek( $this->main_handle, 0 );
	        $header_str = serialize( $this->main_header );
	        fwrite( $this->main_handle, str_pad( $header_str, WECLU_HEADER_BLOCK_SIZE, ' ' ), WECLU_HEADER_BLOCK_SIZE );
	    }
	}
	
	/**
	 * 关闭文件存储引擎 并解锁
	 * @param handle  文件句柄
	 */
	public function close() {
        if ( ! empty( $this -> byte_handle ) ) fclose( $this -> byte_handle );
        if ( ! empty( $this -> main_handle ) ) fclose( $this -> main_handle );
        if ( ! empty( $this -> data_handle ) ) fclose( $this -> data_handle );
        unset( $this -> byte_handle );
        unset( $this -> main_handle );
        unset( $this -> data_handle );
        $this->data_area = 0;
        $this->main_header = null;
	}
	
	/**
	 * 数据压缩
	 */
	public function gzip( $data ) {
	    $data = serialize( $data );
	    return $data;
	}
	
	/**
	 * 数据解压
	 */
	public function unzip( $data ) {
	    $data = unserialize( $data );
	    return $data;
	}
	
	
	/****************************************************************************************
	 * 空间分配管理
	 ****************************************************************************************/
	
	/**
	 * 通过位图地址定位空间的区域, 固定一条落区规则
	 * @param ptr int 位图中的坐标
	 * @param size int
	 */
	public function get_area( $ptr ) {
		//首尾都必须要落在同一片区域， 否则就产生了冲突
		return ceil( ( $ptr + 1 ) / WECLU_TABLE_FILE_MAX_BLOCKS );
	}
	
	/**
	 * 检测连续的区域块是否跨界
	 * @param ptr int 位图中的坐标
	 * @param size int
	 */
	public function is_crossing( $ptr, $size ) {
		//获取头部所在的区域
		if ( ceil( ( $ptr + 1 ) / WECLU_TABLE_FILE_MAX_BLOCKS ) != ceil( ( $ptr + $size ) / WECLU_TABLE_FILE_MAX_BLOCKS ) )
			return true;
		else return false;
	}
	
	/**
	 * 在磁盘存储区中分配一块长度为“size”单位的连续区域，返回该区域的首地址
	 * @param start int 从start位置开始寻找空闲地址
	 */
	public function malloc( $size, $start=-1024 ) {
		//查找位图， 通过位图分配
		if ( $start < 0 ) {
		    fseek( $this->byte_handle, $start, SEEK_END );
		    $start = ftell( $this->byte_handle );
		} else fseek( $this->byte_handle, $start );
	    $ptr = $start;
	    $searchStr = '';
	    $needSpace = str_pad( '0', $size, '0' );
	    $moveSize = 1024;
		do {
		    if ( !empty( $searchStr ) ) {
		        //重复利用前面 1/3的字符 接入到下一截字符串
		        $subStrSize = $size - 1;
		        $searchStr = $subStrSize < 1 ? '' : substr( $searchStr, -$subStrSize );
		        $ptr -= $subStrSize;
		    }
		    $byteStr = trim( fread( $this->byte_handle, $moveSize ) );
		    $byteLen = strlen( $byteStr );
		    $searchStr .= $byteStr;
		    if ( $byteLen < $moveSize ) {
		    	//先判断现成空间能不能找到
		    	$search_pos = strpos( $searchStr, $needSpace );
		    	if ( $search_pos !== false ) {
		    		$ptr += $search_pos;
		    		fseek( $this->byte_handle, $ptr );
		    		fwrite( $this->byte_handle, str_pad( '1', $size, '1' ), $size );
		    		break;
		    	}
		    	$newByteStr = str_pad( '0', $moveSize - $byteLen, '0' );
		    	fwrite( $this->byte_handle, $newByteStr, $moveSize - $byteLen );
		    	$searchStr .= $newByteStr;
		    }
		    
            $search_pos = strpos( $searchStr, $needSpace );
            if ( $search_pos !== false ) {
                $ptr += $search_pos;
                fseek( $this->byte_handle, $ptr );
                fwrite( $this->byte_handle, str_pad( '1', $size, '1' ), $size );
                break;
            }
            
		    $ptr += strlen( $searchStr );
		} while ( true );
		
		//判断是否已经跨界
		if ( $this -> is_crossing( $ptr, $size ) ) {
			$ptr = $this -> malloc( $size, $ptr + $size );
		}
		return $ptr;
	}
	
	/**
	 * 将ptr内存大小增大到size
	 * @param ptr int 空间的首地址指针
	 * @param size int
	 */
	public function realloc( $ptr, $old_size, $new_size ) {
		//首先判断此地址是否有size个空闲空间
		if ( $new_size > $old_size ) {
			//指针移到旧空间的末尾位置
			$needSpace = str_pad( '0', $new_size, '0' );
			$oldSpace	= str_pad( '0', $old_size, '0' );
			//前后偏移300个空间
			$offset = 300;
			$leftSize = $ptr > $offset ? $offset : $ptr;
			fseek( $this->byte_handle, $ptr - $leftSize );
			$searchSpace = fread( $this->byte_handle, $leftSize + $old_size + $offset );
			$searchSpace = substr_replace( $searchSpace, $oldSpace, $leftSize, intval( $old_size ) );
			
			$search_pos = strpos( $searchSpace, $needSpace );
			if ( $search_pos !== false ) {
			    $searchSpace = substr_replace( $searchSpace, str_pad( '1', $new_size, '1' ), $search_pos, intval( $new_size ) );
				fseek( $this->byte_handle, $ptr - $leftSize );
				fwrite( $this->byte_handle, $searchSpace, 1024 );
				$ptr = $ptr - $leftSize + $search_pos;
			} else {
				//释放原来空间
				fseek( $this->byte_handle, $ptr );
				fwrite( $this->byte_handle, $oldSpace, $old_size );
				//分配新空间
				$ptr = $this -> malloc( $new_size );
			}
			
			//判断是否已经跨界
			if ( $this -> is_crossing( $ptr, $new_size ) ) {
				$ptr = $this -> malloc( $new_size, $ptr + $new_size );
			}
			
		} elseif( $old_size > $new_size ) {
			//移到新空间末尾
			fseek( $this->byte_handle, $ptr + $new_size );
			//释放剩余的空间
			fwrite( $this->byte_handle, str_pad( '0', $old_size - $new_size, '0' ), $old_size - $new_size );
		}
		return $ptr;
	}
	
	/**
	 * 在磁盘存储区中分配n块长度为“size”单位的连续区域，返回n首地址 数组
	 * @param n int 分配的空间块数
	 * @param size int
	 */
	public function calloc( $n, $size ) {
		$i = 0;
		$posArr = array();
		while ( $size > $i) {
			$posArr[$i] = $this->malloc( $size, $i > 0 ? $posArr[$i-1] : 0 );
			$i++;
		}
		return $posArr;
	}
	
	/**
	 * 释放存储空间
	 * @param ptr 空间的首地址指针
	 */
	public function free( $ptr, $size ) {
		$this -> set_byte_handle();
		//定位到空间位置
		fseek( $this->byte_handle, $ptr );
		//释放空间
		fwrite( $this->byte_handle, str_pad( '0', $size, '0' ), $size );
	}
	
	
	/****************************************************************************************
	 * 主表存储管理
	 ****************************************************************************************/
	
	/**
	 * 通过ID获取主表的物理地址
	 */
	private function get_main_index( $id ) {
	    $block = WECLU_DATATYPE_SIZE + WECLU_RDS_ID_LENGTH + WECLU_RDS_MAP_ADDR + WECLU_RDS_DATA_BLOCKS;
	    return array( ( $id - 1 ) * $block + WECLU_HEADER_BLOCK_SIZE, $block );
	}
	
	/**
	 * 获取所有的Id
	 */
	public function fetch_ids( $start=1, $end=100000 ) {
		$idArr = array();
		$data_min_block = WECLU_DATATYPE_SIZE + WECLU_RDS_ID_LENGTH + WECLU_RDS_MAP_ADDR + WECLU_RDS_DATA_BLOCKS;
		$prepage = 100000;
		$intercept = $end - $start + 1;
		$page = ceil( $intercept / $prepage );
		for ( $i=1; $i<=$page; $i++ ) {
			
			$start += ( $i - 1 ) * $prepage;
			list( $index, $block ) = $this -> get_main_index( $start );    //转化为物理位置
			$block = ( $intercept - ( $i - 1 ) * $prepage ) * $data_min_block;
			flock( $this->main_handle, LOCK_SH );          //上锁
			fseek( $this->main_handle, $index );
			$m_str = trim( fread( $this->main_handle, $block ) );     //通过index获取主表信息
			flock( $this->main_handle, LOCK_UN );
			$j=0;
			do {
				$p = $j * $data_min_block;
				$m_id = substr( $m_str, $p + WECLU_DATATYPE_SIZE, WECLU_RDS_ID_LENGTH );
				if ( empty( $m_id ) ) break;
				$j++;
				if ( $m_str[$p] != 1 ) continue;
				$idArr[] = ltrim( $m_id, '0' );
			} while( true );
		}
		return $idArr;
	}
	
	/**
	 * 通过主表ID信息批量访问主表数据信息
	 * 注意： 此处id是32进制
	 */
	public function fetch_main_batch( $ids ) {
	    $data = array();
		if ( ! is_array( $ids ) ) $ids = array( $ids );
		$this -> set_main_handle();           //设置主数据句柄
		foreach ( $ids as $id ) {
		    $id = base_convert( $id, 32, 10 );	//将32 进制 转换成10进制
			list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
			flock( $this->main_handle, LOCK_SH );          //上锁
			fseek( $this->main_handle, $index );
				
			$m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
			$type = substr( $m_str, 0, WECLU_DATATYPE_SIZE );
			if ( $type != '1' ) {
				flock( $this->main_handle, LOCK_UN );
				continue;
			}
			$ptr  = substr( $m_str, - WECLU_RDS_MAP_ADDR - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_MAP_ADDR );
			$size = substr( $m_str, - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_DATA_BLOCKS );
			//将32进制转化为10进制
			$ptr = base_convert( $ptr, 32, 10 );
			$size = base_convert( $size, 32, 10 );
			$data[$id] = $this -> fetch_data( $ptr, $size );
			flock( $this->main_handle, LOCK_UN );
		}
		return $data;
	}
	
	/**
	 * 通过主表ID信息访问主表数据信息
	 */
	public function fetch_main( $id ) {
		list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
		
		$this -> set_main_handle();           //设置主数据句柄
		flock( $this->main_handle, LOCK_SH );          //上锁
		fseek( $this->main_handle, $index );

		$m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
		$type = substr( $m_str, 0, WECLU_DATATYPE_SIZE );
		if ( $type != '1' ) {
			flock( $this->main_handle, LOCK_UN );
			return array();
		}
		$m_id = substr( $m_str, WECLU_DATATYPE_SIZE, WECLU_RDS_ID_LENGTH );
		$ptr  = substr( $m_str, - WECLU_RDS_MAP_ADDR - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_MAP_ADDR );
		$size = substr( $m_str, - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_DATA_BLOCKS );
		//将32进制转化为10进制
		$m_id = base_convert( $m_id, 32, 10 );
		$ptr = base_convert( $ptr, 32, 10 );
		$size = base_convert( $size, 32, 10 );
		$data = $this -> fetch_data( $ptr, $size );
		flock( $this->main_handle, LOCK_UN );
		return $data;
	}
	
	/**
	 * 插入一条信息
	 */
	public function insert_main( $data ) {
	    $this -> set_main_handle();    //设置主数据句柄
	    flock( $this->main_handle, LOCK_EX );          //上锁
	    list( $ptr, $size ) = $this -> insert_data( $data );  //先向数据表中插入数据 返回位图坐标
	    
	    //将10进制转化为32进制
	    $ptr = base_convert( $ptr, 10, 32 );
	    $size = base_convert( $size, 10, 32 );
	    
        $this -> get_main_header();   //获取头部信息到缓冲
        $n_id = ++$this->main_header['n_id'];
        $this -> set_main_header();     //  最新ID变更，生效头部信息
        $m_id = base_convert( $n_id, 10, 32 );  //最新Id 32进制
        //写入主表数据
        $main_str = str_pad( '1', WECLU_DATATYPE_SIZE, '0', STR_PAD_LEFT );
        $main_str .= str_pad( $m_id, WECLU_RDS_ID_LENGTH, '0', STR_PAD_LEFT );
        $main_str .= str_pad( $ptr, WECLU_RDS_MAP_ADDR, '0', STR_PAD_LEFT );
        $main_str .= str_pad( $size, WECLU_RDS_DATA_BLOCKS, '0', STR_PAD_LEFT );
        list( $index, $block ) = $this -> get_main_index( $n_id );
        fseek( $this->main_handle, $index );
        fwrite( $this->main_handle, $main_str, $block );
        
	    flock( $this->main_handle, LOCK_UN );          //解锁
	    return $n_id;
	}
	
	/**
	 * 修改一条数据
	 */
	public function update_main( $id, $data ) {
	    list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
		
		$this -> set_main_handle();           //设置主数据句柄
		flock( $this->main_handle, LOCK_EX );
		fseek( $this->main_handle, $index );
		
		$m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
		
		$type = substr( $m_str, 0, WECLU_DATATYPE_SIZE );
		if ( $type != '1' ) {
			flock( $this->main_handle, LOCK_UN );
			return false;
		}
		
		$old_ptr  = substr( $m_str, - WECLU_RDS_MAP_ADDR - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_MAP_ADDR );
		$old_size = substr( $m_str, - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_DATA_BLOCKS );
		//将32进制转化为10进制
		$old_ptr = base_convert( $old_ptr, 32, 10 );
		$old_size = base_convert( $old_size, 32, 10 );
	    
		list( $ptr, $size ) = $this -> update_data( $data, $old_ptr, $old_size );
		
		$m_id = base_convert( $id, 10, 32 );
		$ptr  = base_convert( $ptr, 10, 32 );
		
		$main_str = str_pad( '1', WECLU_DATATYPE_SIZE, '0', STR_PAD_LEFT );
		$main_str .= str_pad( $m_id, WECLU_RDS_ID_LENGTH, '0', STR_PAD_LEFT );
		$main_str .= str_pad( $ptr, WECLU_RDS_MAP_ADDR, '0', STR_PAD_LEFT );
		$main_str .= str_pad( $size, WECLU_RDS_DATA_BLOCKS, '0', STR_PAD_LEFT );
		
		//修改主表头指针
		fseek( $this->main_handle, $index );
		fwrite( $this->main_handle, $main_str, $block );     //通过index获取主表信息
		flock( $this->main_handle, LOCK_UN );
		
		return $id;
	}
	
	/**
	 * 删除主表数据
	 */
	public function delete_main( $id ) {
	    $this->data_area = 0;
	    list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
	    
	    $this -> set_main_handle();           //设置主数据句柄
	    
	    flock( $this->main_handle, LOCK_EX );
	    fseek( $this->main_handle, $index );
	    $m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
	    
	    $type = substr( $m_str, 0, WECLU_DATATYPE_SIZE );
	    if ( $type != '1' ) {
	    	flock( $this->main_handle, LOCK_UN );
	    	return false;
	    }
	    
	    $ptr  = substr( $m_str, - WECLU_RDS_MAP_ADDR - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_MAP_ADDR );
	    $size = substr( $m_str, - WECLU_RDS_DATA_BLOCKS, WECLU_RDS_DATA_BLOCKS );
	    //将32进制转化为10进制
	    $ptr = base_convert( $ptr, 32, 10 );
	    $size = base_convert( $size, 32, 10 );
	    
	    fseek( $this->main_handle, $index );
	    $m_str = fwrite( $this->main_handle, str_pad( '0', WECLU_DATATYPE_SIZE, '0' ), WECLU_DATATYPE_SIZE );     //通过index获取主表信息
	    
	    $this -> free( $ptr, $size);
	    flock( $this->main_handle, LOCK_UN );
	    return true;
	}
	
	
	
	
	/****************************************************************************************
	 * 数据存储管理
	 ****************************************************************************************/
	
	
	/**
	 * 通过地址获取数据信息
	 */
	public function fetch_data( $ptr, $size ) {
	    //定位数据区域
	    $area = $this -> get_area( $ptr );
	    //设置数据句柄
	    $this -> set_data_handle( $area );
	    $area_index = ( $ptr - ( $area - 1 ) * WECLU_TABLE_FILE_MAX_BLOCKS ) * WECLU_RDS_BLOCK_SIZE;
	    fseek( $this->data_handle, $area_index );
	    $data = trim( fread( $this->data_handle, $size * WECLU_RDS_BLOCK_SIZE ) );
	    return $this->unzip( $data );
	}
	
	/**
	 * 通过地址获取数据信息
	 */
	public function insert_data( $data ) {
	    // 压缩数据
	    $data_str = $this->gzip( $data );
	    // 计算数据字符长度
	    $data_len = strlen( $data_str );
	    //需要的数据块
	    $size = ceil( $data_len / WECLU_RDS_BLOCK_SIZE );
	    
	    $this -> set_byte_handle();            //设置位图句柄， 准备查找地图
	    
	    $ptr = $this -> malloc( $size );              //分配空间
	    
	    //定位数据区域
	    $area = $this -> get_area( $ptr );
	    //设置数据句柄
	    $this -> set_data_handle( $area );
	    
	    //获取区域中的位置坐标
	    $area_index = ( $ptr - ( $area - 1 ) * WECLU_TABLE_FILE_MAX_BLOCKS ) * WECLU_RDS_BLOCK_SIZE;
	    fseek( $this->data_handle, $area_index );      //指针指向该位置
	    //开始写入数据
	    $blockSize = WECLU_RDS_BLOCK_SIZE * $size;
	    fwrite( $this->data_handle, str_pad( $data_str, $blockSize, ' ' ), $blockSize );
	    
	    return array( $ptr, $size );
	}
	
	/**
	 * 通过地址修改数据信息
	 */
	public function update_data( $data, $old_ptr, $old_size ) {
	    // 压缩数据
	    $data_str = $this->gzip( $data );
	    // 计算数据字符长度
	    $data_len = strlen( $data_str );
	    //需要的数据块
	    $size = ceil( $data_len / WECLU_RDS_BLOCK_SIZE );
	    $this -> set_byte_handle();            //设置位图句柄， 准备查找地图
	    
	    $ptr = $this -> realloc( $old_ptr, $old_size, $size);              //分配空间
	    
	    //定位数据区域
	    $area = $this -> get_area( $ptr );
	    //设置数据句柄
	    $this -> set_data_handle( $area );
	     
	    //获取区域中的位置坐标
	    $area_index = ( $ptr - ( $area - 1 ) * WECLU_TABLE_FILE_MAX_BLOCKS ) * WECLU_RDS_BLOCK_SIZE;
	    fseek( $this->data_handle, $area_index );      //指针指向该位置
	    //开始写入数据
	    $blockSize = WECLU_RDS_BLOCK_SIZE * $size;
	    fwrite( $this->data_handle, str_pad( $data_str, $blockSize, ' ' ), $blockSize );
	    
	    return array( $ptr, $size );
	}
	
}
