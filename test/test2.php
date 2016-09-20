<?php
include_once './interface.class.php';
$weclu = new WECLU( 'storage' );
//$weclu -> init_space();exit;
$weclu -> create( 'st2', 'HASH' );
$t1 = microtime(true);
//测试结果：最佳数据量为连续插入4W, 耗时5S
for( $i=1410000; $i<1410000; $i++ ) {
	$weclu->set( 'st2', 'name' . $i, 'zoujunrong' . $i );
	//$re = $weclu->set( 'st2', 'name0', 'zjr0' );
	//$re = $weclu->set( 'st2', 'name31', 'zjr31' );
	//$re = $weclu->get( 'st2', 'name' . $i );
	//if ( ! $re ) echo 'name' . $i . "\n";
}/**/
//测试结果: 137W数据中查询 1ms, 随着数据增长，呈现常数
$i = 1370000 - 5;
//$weclu->set( 'st2', 'name' . $i, 'zoujunrong' . $i );
echo $weclu->get( 'st2', 'name' . $i ) . "\n";
//print_r($res);
$t2 = microtime(true);
echo $t2 - $t1; echo "\n";
exit;

