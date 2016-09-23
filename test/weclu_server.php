<?php
include_once '/data/web/weclu/weclu.class.php';
global $connArr;
$connArr = array();
$serv = new swoole_server( "0.0.0.0", 9501 );
$serv->set( array( 'daemonize' => 0) );
$serv->on( 'connect', function ( $serv, $fd ) {
	echo "Client:Connect.\n";
});

//接收消息事件
$serv->on('receive', function ( $serv, $fd, $from_id, $data ) {
	if ( $data == 'reload' ) {
		echo "reload... \n";
		$serv -> reload();	//重启
		$serv->send($fd, 'Service has reloaded');
	} elseif ( $data == 'stop' ) {
		echo "stop... \n";
		$serv -> stop();	//停止
		$serv->send($fd, 'Service has stoped');
	} elseif ( $data == 'shutdown' ) {
		echo "shutdown... \n";
		$serv -> shutdown();	//关机
		$serv->send($fd, 'Service has shutdown');
	} else {
	    global $connArr;
	    $data = json_decode( $data, true );
	    $key = md5( $fd );
	    if ( isset( $connArr[$key] ) ) {
	        $retArr['code'] = 0;
	        $retArr['msg'] = '';
	        $retArr['data'] = '';
	        //取数据
	        $query = key( $data );
	        $params = $data[$query];
	        
	        if ( ! is_array( $params ) ) {
	        	$retArr['msg'] = 'params need array type!';
	        } else {
	        	switch ( $query ) {
	        		case 'use_db':
	        			if ( ! empty( $params ) && is_string( $params ) ) {
	        				$connArr[$key] -> $query( $params );
	        				$retArr['msg'] = 'use database success';
	        				$retArr['code'] = 0;
	        			} else {
	        				$retArr['msg'] = 'invalid params!';
	        				$retArr['code'] = 201;
	        			}
	        			break;
	        		case 'create':
	        			$retArr['code'] = 202;
	        			if ( empty( $params[0] ) ) {
	        				$retArr['msg'] = 'lost of table!';
	        			} elseif ( empty( $params[1] ) ) {
	        				$retArr['msg'] = 'lost of engine!';
	        			} else {
	        				$retArr['code'] = 0;
	        				$startId = isset( $params[2] ) && is_integer( $params[2] ) ? $params[2] : 1;
	        				if ( ! $connArr[$key] -> $query( $params[0], $params[1], $startId ) ) {
	        					$retArr['msg'] = 'table has exist!';
	        				}
	        			}
	        			break;
	        		case 'show_databases':
	        			$dbs = $connArr[$key] -> $query();
	        			$retArr['data'] = $dbs;
	        			break;
	        		case 'show_tables':
	        			$tables = $connArr[$key] -> $query();
	        			$retArr['data'] = $tables;
	        			break;
	        		case 'init_space':
	        			$connArr[$key] -> $query();
	        			break;
	        		case 'set':
	        			$retArr['code'] = 206;
	        			if ( empty( $params[0] ) ) {
	        				$retArr['msg'] = 'lost of table!';
	        			} elseif ( empty( $params[1] ) ) {
	        				$retArr['msg'] = 'lost of key!';
	        			} elseif ( ! isset( $params[2] ) ) {
	        				$retArr['msg'] = 'lost of value!';
	        			} else {
	        				$retArr['code'] = 0;
	        				$expire = isset( $params[3] ) && is_integer( $params[3] ) ? $params[3] : 0;
	        				$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1], $params[2], $expire );
	        			}
	        			break;
        			case 'sets':
        				$retArr['code'] = 207;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) || ! is_array( $params[1] ) ) {
        					$retArr['msg'] = 'lost of keyArr!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
        			case 'get':
        				$retArr['code'] = 208;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) ) {
        					$retArr['msg'] = 'lost of key!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
        			case 'gets':
        				$retArr['code'] = 209;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) || ! is_array( $params[1] ) ) {
        					$retArr['msg'] = 'lost of keys!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
        				
        			case 'create_index':
        				$retArr['code'] = 210;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) || ! is_array( $params[1] ) ) {
        					$retArr['msg'] = 'lost of keys!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
        			case 'delete_index':
        				$retArr['code'] = 211;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) || ! is_array( $params[1] ) ) {
        					$retArr['msg'] = 'lost of keys!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
        			case 'select':
        				$retArr['code'] = 212;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) ) {
        					$retArr['msg'] = 'lost of filed!';
        				} elseif ( empty( $params[2] ) || ! is_array( $params[2] ) ) {
        					$retArr['msg'] = 'lost of where!';
        				} else {
        				    $sort = isset( $params[3] ) && is_array( $params[3] ) ? $params[3] : array();
        				    $page = isset( $params[4] ) && is_integer( $params[4] ) ? $params[4] : 0;
        				    $prepage = isset( $params[5] ) && is_integer( $params[5] ) ? $params[5] : 1000;
        				    
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1], $params[2], $sort, $page, $prepage );
        				}
        				break;
        			case 'insert':
        				$retArr['code'] = 213;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) ) {
        					$retArr['msg'] = 'lost of data!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
        			case 'update':
        				$retArr['code'] = 214;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) || ! is_array( $params[1] ) ) {
        					$retArr['msg'] = 'lost of data!';
        				} elseif ( empty( $params[2] ) || ! is_array( $params[2] ) ) {
        					$retArr['msg'] = 'lost of where!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1], $params[2] );
        				}
        				break;
        			case 'delete':
        				$retArr['code'] = 215;
        				if ( empty( $params[0] ) ) {
        					$retArr['msg'] = 'lost of table!';
        				} elseif ( empty( $params[1] ) || ! is_array( $params[1] ) ) {
        					$retArr['msg'] = 'lost of where!';
        				} else {
        					$retArr['code'] = 0;
        					$retArr['data'] = $connArr[$key] -> $query( $params[0], $params[1] );
        				}
        				break;
	        		default:
	        			$retArr['code'] = 201;
	        			$retArr['msg']  = 'invalid query!';
	        	}
	        }
	    } else {
	        //获取
	        if ( isset( $data['use_db'] ) ) {
	            $connArr[$key] = new WECLU( $data['use_db'] );
	            $retArr['msg'] = 'use database success';
	            $retArr['code'] = 0;
	        } else {
	            $retArr['code'] = 101;
	            $retArr['msg'] = 'no select database!';
	        }
	    }
	    $retData = json_encode( $retArr );
		$serv->send( $fd, str_pad( strlen( $retData ), 14, '0', STR_PAD_LEFT ) . $retData );
	}
});

//关闭连接事件
$serv->on( 'close', function( $serv, $fd ) {
	echo "Client: Close.\n";
	$key = md5( $fd );
	global $connArr;
	if ( isset( $connArr[$key] ) ) {
	    unset( $connArr[$key] );
	}
});

$serv->start();
