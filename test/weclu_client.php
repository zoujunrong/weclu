<?php
/** 
 * File name:weclu_client.php 
 * @author zjr
 * @since 2016-09-17
 */
set_time_limit(0);  
  
$host = "127.0.0.1";
$port = 9501;
$weclu = new WECLU( $host, $port );
//选择数据库
$weclu->query( 'use_db', 'storage' );
$weclu->query( 'create', array( 'storage', 'RDS' ) );
$weclu->query( 'delete_index', array( 'storage', array( 'record' ) ) );
$weclu->query( 'create_index', array( 'storage', array('name', 'record') ) );

for ( $i=1; $i<10000; $i++ ) {
    $data = array( 'name' => 'zjr', 'record' => $i, 'neck_name' => 'shaofei', 'address' => 'shenzhen' );
    //$insert = $weclu->query( 'insert', array( 'storage', $data ) );
    //print_r($insert);
}
$select = $weclu->query( 'select', array( 'storage', '*', array( 'record' => 9951 ), array(), 1, 10 ) );
print_r($select);

class WECLU {
	
	private $socket = null;
	public function __construct( $host, $port ) {
		$this->socket = socket_create( AF_INET, SOCK_STREAM, SOL_TCP ) or die( "Could not create  socket\n" );
		socket_connect( $this->socket, $host, $port ) or die( 'connect filed!' );
	}
	
	public function __destruct() {
		socket_close( $this->socket );
	}
	
	public function query( $oper, $oper_param=array() ) {
	    $json_data = json_encode( array( $oper => $oper_param ) );
		socket_write( $this->socket, $json_data ) or die( "query weclu failed\n" );
		
		$size = 1024;
		$buff = trim( socket_read( $this->socket, $size, PHP_BINARY_READ ) );
		$strlen = intval( substr( $buff, 0, 14) );
		$data = substr( $buff, 14);
		if ( $strlen > ( $size - 14 ) ) {
			$data .= trim( socket_read( $this->socket, $strlen + 14 - $size, PHP_BINARY_READ ) );
		}
		
		if ( ! empty( $data ) ) {
			$data = json_decode( $data, true );
		}
		return $data;
	}
	
}