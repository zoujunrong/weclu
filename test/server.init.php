<?php
global $oper;
$oper = isset( $argv[1] ) ? $argv[1] : '';
if ( empty( $oper ) || $oper == 'start' ) {
	exec('ps aux|grep swoole.server.php',$ps);
	if ( count( $ps ) < 4 ) {
		exec('php /data/web/zjrdb/test/swoole_server.php',$ps);
		exec('ps aux|grep swoole.server.php',$ps);
		if ( count( $ps ) > 4 ) echo "Service start \n";
		
	} else {
		echo "Service start \n";
	}
	exit;
}
$client = new swoole_client(SWOOLE_SOCK_TCP, SWOOLE_SOCK_ASYNC);
$client->on("connect", function($cli) {
	echo "connect \n";
	global $oper;
    $cli->send( $oper );
});
$client->on("receive", function($cli, $data){
    echo "reponse: $data\n";
    $cli->close();
    exit;
});
$client->on("error", function($cli){
    echo "connect fail\n";
});
$client->on("close", function($cli){
    echo "connect close\n";
});
$client->connect('114.215.116.159', 9501, 0.5);