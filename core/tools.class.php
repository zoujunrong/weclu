<?php
/**
 * @description 索引管理层
 * @author zjr
 * @2016/08/02
 * 说明： 数据库索引抽象层
 */
include 'zjrdb_conf.php';
class Index {
    
	private $table_space = null;
	private $table_type = null;
	
	private $byte_handle = null;
	private $main_handle = null;
	private $data_handle = null;
	
	private $main_header = null;
	private $data_area = 1;		//当前数据区域
	
	public function __construct( $table_space, $table_type = 'RDS' ) {
		if ( ! empty( $table_space ) ) {
			$this -> table_space = $table_space;
			$this -> table_type = $table_type;
		}
	}
	
	
	/**
	 * 设置位图句柄
	 */
	public function set_byte_handle() {
		$byte_path = $this->table_space . 'byte.map';
		//关闭当前数据句柄
		if ( ! empty( $this -> byte_handle ) ) fclose( $this -> byte_handle );
		if ( is_file( $byte_path ) ) {
		    $this -> byte_handle = fopen( $byte_path, 'r+' );
		} else {
		    $this -> byte_handle = fopen( $byte_path, 'w+' );
		}
	}
	
	/**
	 * 设置主表句柄
	 */
	public function set_main_handle() {
		$main_path = $this->table_space . 'main.data';
		//关闭当前数据句柄
		if ( ! empty( $this -> main_handle ) ) fclose( $this -> main_handle );
		if ( is_file( $main_path ) ) {
			$this -> main_handle = fopen( $main_path, 'r+' );
		} else {
			$this -> main_handle = fopen( $main_path, 'w+' );
			$this -> init_main_header();     //初始化头部信息
		}
	}
	
	/**
	 * 初始化主表头部信息
	 */
	public function init_main_header() {
	    $this->main_header = array(
	        's_id'  => 1,       //起始ID
            'n_id'  => 0,       //最新ID
            'type'  => $this->table_type,   //类型
	    );
	    $this -> set_main_header();
	}
	
	/**
	 * 获取主表头部信息
	 */
	public function get_main_header() {
	    fseek( $this->main_handle, 0 );
	    $header = fread( $this->main_handle, ZJRDB_HEADER_BLOCK_SIZE );
	    $this->main_header = unserialize( $header );
	}
	
	/**
	 * 设置主表头部信息
	 */
	public function set_main_header() {
	    if ( ! empty( $this->main_header ) ) {
	        fseek( $this->main_handle, 0 );
	        $header_str = serialize( $this->main_handle );
	        fwrite( $this->main_handle, $header_str, strlen( $header_str ) );
	    }
	}
	
	
	/**
	 * 设置数据句柄
	 */
	public function set_data_handle( $area ) {
		if ( $area != $this->area && $area > 0 ) {
			//关闭当前数据句柄
			if ( ! empty( $this -> data_handle ) ) fclose( $this -> data_handle );
			//开启新区域数据句柄
			$data_path = $this->table_space . $area . '.data';
			if ( is_file( $data_path ) ) {
				$this -> data_handle = fopen( $data_path, 'r+' );
			} else {
				$this -> data_handle = fopen( $$data_path, 'w+' );
			}
		}
	}
	
	/**
	 * 关闭文件存储引擎 并解锁
	 * @param handle  文件句柄
	 */
	public function close( $handle = null ) {
	    if ( ! empty( $handle ) ) fclose( $handle );
	    else {
	        if ( ! empty( $this -> byte_handle ) ) fclose( $this -> byte_handle );
	        if ( ! empty( $this -> main_handle ) ) fclose( $this -> main_handle );
	        if ( ! empty( $this -> data_handle ) ) fclose( $this -> data_handle );
	    }
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
		return ceil( ( $ptr + 1 ) / ZJRDB_TABLE_FILE_MAX_BLOCKS );
	}
	
	/**
	 * 检测连续的区域块是否跨界
	 * @param ptr int 位图中的坐标
	 * @param size int
	 */
	public function is_crossing( $ptr, $size ) {
		//获取头部所在的区域
		if ( ceil( ( $ptr + 1 ) / ZJRDB_TABLE_FILE_MAX_BLOCKS ) != ceil( ( $ptr + $size ) / ZJRDB_TABLE_FILE_MAX_BLOCKS ) )
			return true;
		else return false;
	}
	
	/**
	 * 在磁盘存储区中分配一块长度为“size”单位的连续区域，返回该区域的首地址
	 * @param start int 从start位置开始寻找空闲地址
	 */
	public function malloc( $size, $start=0 ) {
		//查找位图， 通过位图分配
		fseek( $this->handle, $start );
	    $ptr = $start;
	    $searchStr = '';
	    $moveSize = 1024;
		do {
		    if ( !empty( $searchStr ) ) {
		        //重复利用前面 3/1的字符 接入到下一截字符串
		        $searchStr = substr( $searchStr, ceil( $moveSize / 3 ) );
		        $ptr -= ceil( $moveSize / 3 );
		    }
		    $byteStr = fread( $this->byte_handle, $moveSize );
		    $searchStr .= $byteStr;
		    if ( ! empty( $byteStr ) ) {
		        $search_pos = strpos( $searchStr, str_pad( '0', $size ) );
		        if ( $search_pos !== false ) {
		            $ptr += $search_pos;
		            break;
		        }
		    } else {
		        //直到文件末尾都还没有找到空闲空间时，则重新开辟文件空间
		        fwrite( $this->byte_handle, str_pad( '1', $size ), $size );
		        break;
		    }
		    $ptr += strlen( $searchStr );
		} while ( ! empty( $byteStr ) && !feof( $this->byte_handle ) );
		
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
			fseek( $this->byte_handle, $ptr + $old_size );
			$searchStr = fread( $this->byte_handle, $new_size - $old_size );
			if ( base_convert( $searchStr, 2, 10 ) === 0 ) {
				//指针移回到旧空间的末尾位置
				fseek( $this->byte_handle, $ptr + $old_size );
				fwrite( $this->byte_handle, str_pad( '1', $new_size - $old_size ), $new_size - $old_size );
			} else {
				//释放原来空间
				fseek( $this->byte_handle, $ptr );
				fwrite( $this->byte_handle, str_pad( '0', $old_size ), $old_size );
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
			fwrite( $this->byte_handle, str_pad( '0', $old_size - $new_size ), $old_size - $new_size );
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
		//定位到空间位置
		fseek( $this->byte_handle, $ptr );
		//释放空间
		fwrite( $this->byte_handle, str_pad( '0', $size ), $size );
	}
	
	
	/****************************************************************************************
	 * 主表存储管理
	 ****************************************************************************************/
	
	/**
	 * 通过ID获取主表的物理地址
	 */
	private function get_main_index( $id ) {
	    $block = ZJRDB_DATATYPE_SIZE + ZJRDB_RDS_ID_LENGTH + ZJRDB_RDS_MAP_ADDR + ZJRDB_DATATYPE_SIZE;
	    return array( ( $id - 1 ) * $block + ZJRDB_HEADER_BLOCK_SIZE, $block );
	}
	
	/**
	 * 通过主表ID信息访问主表数据信息
	 */
	public function fetch_main( $id ) {
		
		list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
		
		$this -> set_main_handle();           //设置主数据句柄
		fseek( $this->main_handle, $index );

		flock( $this->main_handle, LOCK_SH );
		$m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
		flock( $this->main_handle, LOCK_UN );
		
		$type = substr( $m_str, 0, ZJRDB_DATATYPE_SIZE );
		if ( $type != '1' ) return false;
		
		$m_id = substr( $m_str, ZJRDB_DATATYPE_SIZE, ZJRDB_RDS_ID_LENGTH );
		$ptr  = substr( $m_str, - ZJRDB_RDS_MAP_ADDR - ZJRDB_RDS_DATA_BLOCKS, ZJRDB_RDS_MAP_ADDR );
		$size = substr( $m_str, - ZJRDB_RDS_DATA_BLOCKS, ZJRDB_RDS_DATA_BLOCKS );
		//将32进制转化为10进制
		$m_id = base_convert( $m_id, 32, 10 );
		$ptr = base_convert( $ptr, 32, 10 );
		$size = base_convert( $size, 32, 10 );
		
		$data = $this -> fetch_data( $ptr, $size );
		
		$this -> close();
		
		return $data;
	}
	
	/**
	 * 插入一条信息
	 */
	public function insert_main( $data ) {
	    
	    list( $ptr, $size ) = $this -> insert_data( $data );  //先向数据表中插入数据 返回位图坐标
	    
	    //将10进制转化为32进制
	    $ptr = base_convert( $ptr, 10, 32 );
	    $size = base_convert( $size, 10, 32 );
	    
	    $this -> set_main_handle();    //设置主数据句柄
	    
	    flock( $this->data_handle, LOCK_EX );          //上锁
	    
        $this -> get_main_header();   //获取头部信息到缓冲
        $n_id = ++$this->main_header['n_id'];
        $this -> set_main_header();     //  最新ID变更，生效头部信息
        $m_id = base_convert( $m_id, 10, 32 );  //最新Id 32进制

        //写入主表数据
        $main_str = str_pad( '1', ZJRDB_DATATYPE_SIZE, '0', STR_PAD_LEFT );
        $main_str .= str_pad( $m_id, ZJRDB_RDS_ID_LENGTH, '0', STR_PAD_LEFT );
        $main_str .= str_pad( $ptr, ZJRDB_RDS_MAP_ADDR, '0', STR_PAD_LEFT );
        $main_str .= str_pad( $size, ZJRDB_RDS_DATA_BLOCKS, '0', STR_PAD_LEFT );
        
        fwrite( $this->main_handle, $main_str, strlen( $main_str ) );
        
	    flock( $this->data_handle, LOCK_UN );          //解锁
	    
	    $this -> close();
	    
	    return $n_id;
	}
	
	/**
	 * 修改一条数据
	 */
	public function update_main( $id, $data ) {
	    
	    list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
		
		$this -> set_main_handle();           //设置主数据句柄
		fseek( $this->main_handle, $index );
		
		flock( $this->main_handle, LOCK_SH );
		$m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
		flock( $this->main_handle, LOCK_UN );
		
		$type = substr( $m_str, 0, ZJRDB_DATATYPE_SIZE );
		if ( $type != '1' ) return false;
		
		$old_ptr  = substr( $m_str, - ZJRDB_RDS_MAP_ADDR - ZJRDB_RDS_DATA_BLOCKS, ZJRDB_RDS_MAP_ADDR );
		$old_size = substr( $m_str, - ZJRDB_RDS_DATA_BLOCKS, ZJRDB_RDS_DATA_BLOCKS );
		//将32进制转化为10进制
		$old_ptr = base_convert( $old_ptr, 32, 10 );
		$old_size = base_convert( $old_size, 32, 10 );
	    
		list( $ptr, $size ) = $this -> update_data( $data, $old_ptr, $old_size );
		
		$main_str = str_pad( $ptr, ZJRDB_RDS_MAP_ADDR, '0', STR_PAD_LEFT );
		$main_str .= str_pad( $size, ZJRDB_RDS_DATA_BLOCKS, '0', STR_PAD_LEFT );
		
		//修改主表头指针
		fseek( $this->main_handle, $index + ZJRDB_DATATYPE_SIZE + ZJRDB_RDS_ID_LENGTH );
		flock( $this->main_handle, LOCK_EX );
		fwrite( $this->main_handle, $main_str, ZJRDB_RDS_MAP_ADDR + ZJRDB_RDS_DATA_BLOCKS );     //通过index获取主表信息
		flock( $this->main_handle, LOCK_UN );
		
		$this -> close();
		
		return $id;
	}
	
	/**
	 * 删除主表数据
	 */
	public function delete_main( $id ) {
	    
	    list( $index, $block ) = $this -> get_main_index( $id );    //转化为物理位置
	    
	    $this -> set_main_handle();           //设置主数据句柄
	    fseek( $this->main_handle, $index );
	    
	    flock( $this->main_handle, LOCK_SH );
	    $m_str = fread( $this->main_handle, $block );     //通过index获取主表信息
	    flock( $this->main_handle, LOCK_UN );
	    
	    $type = substr( $m_str, 0, ZJRDB_DATATYPE_SIZE );
	    if ( $type != '1' ) return false;
	    
	    $ptr  = substr( $m_str, - ZJRDB_RDS_MAP_ADDR - ZJRDB_RDS_DATA_BLOCKS, ZJRDB_RDS_MAP_ADDR );
	    $size = substr( $m_str, - ZJRDB_RDS_DATA_BLOCKS, ZJRDB_RDS_DATA_BLOCKS );
	    //将32进制转化为10进制
	    $ptr = base_convert( $ptr, 32, 10 );
	    $size = base_convert( $size, 32, 10 );
	    
	    fseek( $this->main_handle, $index );
	    flock( $this->main_handle, LOCK_EX );
	    $m_str = fwrite( $this->main_handle, str_pad( '0', ZJRDB_DATATYPE_SIZE, '0' ), ZJRDB_DATATYPE_SIZE );     //通过index获取主表信息
	    flock( $this->main_handle, LOCK_UN );
	    
	    $this -> free( $ptr, $size);
	    $this -> close();
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
	    
	    $area_index = ( $ptr - ( $area - 1 ) * ZJRDB_TABLE_FILE_MAX_BLOCKS ) * ZJRDB_RDS_BLOCK_SIZE;
	    fseek( $this->data_handle, $area_index );
	    flock( $this->data_handle, LOCK_SH );
	    $data = fread( $this->data_handle, $size * ZJRDB_RDS_BLOCK_SIZE );
	    flock( $this->data_handle, LOCK_UN );
	    $this -> close( $this->data_handle );
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
	    $size = ceil( $data_len / ZJRDB_RDS_BLOCK_SIZE );
	    
	    $this -> set_byte_handle();            //设置位图句柄， 准备查找地图
	    
	    flock( $this->byte_handle, LOCK_EX );  //分配空间前上写锁
	    $ptr = $this -> malloc( $size );              //分配空间
	    flock( $this->byte_handle, LOCK_UN );  //解锁
	    
	    $this -> close( $this->byte_handle );  // 关闭位图句柄
	    
	    //定位数据区域
	    $area = $this -> get_area( $ptr );
	    //设置数据句柄
	    $this -> set_data_handle( $area );
	    
	    //获取区域中的位置坐标
	    $area_index = ( $ptr - ( $area - 1 ) * ZJRDB_TABLE_FILE_MAX_BLOCKS ) * ZJRDB_RDS_BLOCK_SIZE;
	    fseek( $this->data_handle, $area_index );      //指针指向该位置
	    //开始写入数据
	    flock( $this->data_handle, LOCK_EX );  //上锁
	    fwrite( $this->data_handle, $data_str, $data_len );
	    flock( $this->data_handle, LOCK_UN );  //解锁
	    
	    $this -> close( $this->data_handle );  //关闭数据句柄
	    
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
	    $size = ceil( $data_len / ZJRDB_RDS_BLOCK_SIZE );
	    $this -> set_byte_handle();            //设置位图句柄， 准备查找地图
	     
	    flock( $this->byte_handle, LOCK_EX );  //分配空间前上写锁
	    $ptr = $this -> realloc( $old_ptr, $old_size, $size);              //分配空间
	    flock( $this->byte_handle, LOCK_UN );  //解锁
	    $this -> close( $this->byte_handle );  //关闭位图句柄
	    ;
	    //定位数据区域
	    $area = $this -> get_area( $ptr );
	    //设置数据句柄
	    $this -> set_data_handle( $area );
	     
	    //获取区域中的位置坐标
	    $area_index = ( $ptr - ( $area - 1 ) * ZJRDB_TABLE_FILE_MAX_BLOCKS ) * ZJRDB_RDS_BLOCK_SIZE;
	    fseek( $this->data_handle, $area_index );      //指针指向该位置
	    //开始写入数据
	    flock( $this->data_handle, LOCK_EX );  //上锁
	    fwrite( $this->data_handle, $data_str, $data_len );
	    flock( $this->data_handle, LOCK_UN );  //解锁
	    $this -> close( $this->data_handle );  //关闭数据句柄
	    
	    return array( $ptr, $size );
	}
	
}
