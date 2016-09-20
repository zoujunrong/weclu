<?php
/**
 * @description 接口管理层
 * @author zjr
 * @2016/08/02
 * 说明： 数据库接口抽象层
 */
include 'pager.class.php';
class WECLU {
    private $pager = null;
	public function __construct( $db ) {
	    $this->pager = new Pager( $db );
	}
	
	/**
	 * 切换数据库
	 * @param string $db
	 */
	public function change_db( $db ) {
	    $this->pager->change_space( $db );
	}
	
	/**
	 * 创建数据表
	 * @param string $table
	 * @param string $engine
	 * @param string $search_key
	 * @param number $start_id
	 */
	public function create( $table, $engine, $start_id=1 ) {
	    return $this->pager->create( $table, $engine, $start_id );
	}
	
	public function show_databases() {
	    return $this->pager->show_databases();
	}
	
	public function show_tables() {
	    return $this->pager->show_tables();
	}
	
	
	/****************************************************************************************************
	 * HASH引擎
	 */
	
	public function init_space() {
		$this->pager->init_hash_space();
	}
	
	public function set( $table, $key, $val, $expire=0 ) {
		$data = $this->pager->sets( $table, array( $key => array( $val, $expire ) ) );
		return isset( $data[$key] ) ? $data[$key] : 0;
	}
	
	public function sets( $table, $keyArr ) {
		return $this->pager->sets( $table, $keyArr );
	}
	
	public function get( $table, $key ) {
		$data = $this->pager->gets( $table, array( $key ) );
		return isset( $data[$key] ) ? $data[$key] : '';
	}
	
	public function gets( $table, $keys ) {
		return $this->pager->gets( $table, $keys );
	}
	
	/****************************************************************************************************
	 * RDS引擎
	 */
	
	/**
	 * 创建索引
	 * @param string $table
	 * @param array $indexs
	 * @return array 返回创建成功的索引
	 */
	public function create_index( $table, $indexs ) {
		return $this->pager->create_index( $table, $indexs );
	}
	
	/**
	 * 删除索引
	 * @param string $table
	 * @param array $indexs
	 * @return array 返回删除成功的索引
	 */
	public function delete_index( $table, $indexs ) {
		return $this->pager->delete_index( $table, $indexs );
	}
	
	/**
	 * 查询
	 * @param unknown $filed
	 * @param unknown $table
	 */
	public function select( $table, $filed, $where, $sort=array(), $page=0, $prepage=1000  ) {
		//拆解filed条件
		$filed = weclu_filter_filed( $filed );
		return $this->pager->select( $table, $filed, $where, $sort, $page, $prepage );
	}
	
	public function insert( $table, $data ) {
		return $this->pager->insert( $table, $data );
	}
	
	/**
	 * 修改数据
	 * @param string $table 表名
	 * @param array $where 查询条件
	 * @return array
	 * 返回修改成功的id
	 */
	public function update( $table, $data, $where ) {
	    return $this->pager->update( $table, $data, $where );
	}
	
	/**
	 * 删除数据
	 * @param string $table
	 * @param array $where
	 * @return array 
	 * 返回删除成功的id
	 */
	public function delete( $table, $where ) {
		return $this->pager->delete( $table, $where );
	}
	
}
