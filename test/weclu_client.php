<?php
/** 
 * File name:weclu_client.php 
 * @author zjr
 * @since 2016-09-17
 */
set_time_limit(0);  
  
$host = "114.215.116.159";  
//$host = "127.0.0.1";
$port = 9501;

class WECLU {
	
	private $socket = null;
	public function __construct( $host, $port ) {
		$this->socket = socket_create( AF_INET, SOCK_STREAM, SOL_TCP )or die( "Could not create  socket\n" );
	}
	
	public function __destruct() {
		socket_close( $this->socket );
	}
	
	private function query( $json_data ) {
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