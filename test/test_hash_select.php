<?php
include_once '../weclu.class.php';
include_once 'mysql.php';
$weclu = new WECLU( 'storage');

$start = 1300000;
$end = $start + 100000;
/*
$t1 = microtime(true);
for($i=$start; $i<$end; $i++){
	$data = array( 'name' => array( 'd' => 'zjr', 'c' => 'a'.$i ), 'sex' => 'man'.$i );
	$res = $weclu->get( 'hash', 'key'.$i );
	//if ( empty($res) ) echo  'key'.$i . "\n";
	//$weclu->insert( 'rds', $data );
}

$t2 = microtime(true);
echo ($t2-$t1) . "\n";exit;
*/
$t1 = microtime(true);
for($i=$start; $i<$end; $i++){
	$data = array( 'name' => array( 'd' => 'zjr', 'c' => 'a'.$i ), 'sex' => 'man'.$i );
	$res = $weclu->select( 'rds', '*', array('sex' => 'man'.$i) );
}
$t2 = microtime(true);
echo ($t2-$t1) . "\n";

exit;

$dbConn	=	new mysql();
$dbConn->connect( 'localhost', 'root', '123019' );
$dbConn->select_db( 'test' );

$t1 = microtime(true);
for($i=$start; $i<$end; $i++){
	$sql = "select * from test1 where sex = 'man{$i}'";
	$dbConn->query($sql);
}
$t2 = microtime(true);
echo ($t2-$t1) . "\n";