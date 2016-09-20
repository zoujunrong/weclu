<?php
$serv = new swoole_server( "0.0.0.0", 9501 );
$serv->set(array('daemonize' => 1));
$serv->on('connect', function ($serv, $fd){
	echo "Client:Connect.\n";
});

//接收消息事件
$serv->on('receive', function ($serv, $fd, $from_id, $data) {
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
		$data = json_decode( $data, true );
		
		$serv->send($fd, str_pad( strlen( $data ), 14, '0', STR_PAD_LEFT ) . $data);
	}
});

//关闭连接事件
$serv->on('close', function ($serv, $fd) {
	echo "Client: Close.\n";
});

$serv->start();
