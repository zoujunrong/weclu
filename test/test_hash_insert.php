<?php
include_once '../weclu.class.php';
include_once 'mysql.php';
$weclu = new WECLU( 'storage');
/*$weclu->init_space();//exit;
$weclu->create( 'hash', 'HASH' );
$weclu->create( 'rds', 'RDS' );
$weclu->create_index( 'rds', array('name.c', 'sex') );exit;
*/

$start = 80000;
$end = $start + 10000;

$t1 = microtime( true );
for($i=$start; $i<$end; $i++){
	$data = array( 'name' => array( 'd' => 'zjr', 'c' => 'a'.$i ), 'sex' => 'man'.$i, 'nuksssssssssssssnuksssssssssssssnuksssssssssssssnukssssss' );
	//$weclu->set( 'hash', 'key'.$i, json_encode($data) );
	/**/$res = $weclu->get( 'hash', 'key'.$i );
	if (!empty($res)) {
		echo $res. "\n";exit;
	}
	//$weclu->insert( 'rds', $data );
}

$t2 = microtime(true);
echo ($t2-$t1) . "\n";exit;
/*
$t1 = microtime(true);
for($i=$start; $i<$end; $i++){
	$data = array( 'name' => array( 'd' => 'zjr', 'c' => 'a'.$i ), 'sex' => 'man'.$i );
	//$weclu->set( 'hash', 'key'.$i, json_encode($data) );
	$weclu->insert( 'rds', $data );
}
$t2 = microtime(true);
echo ($t2-$t1) . "\n";

exit;*/

$dbConn	=	new mysql();
$dbConn->connect( 'localhost', 'root', '123019' );
$dbConn->select_db( 'test' );

$t1 = microtime(true);
for($i=$start; $i<$end; $i++){
	$sql = "INSERT INTO test1 (name, sex, d) VALUES('a{$i}', 'man{$i}', 'zjr')";
	$dbConn->query($sql);
}
$t2 = microtime(true);
echo ($t2-$t1) . "\n";