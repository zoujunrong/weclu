<?php
include_once './interface.class.php';
$storage = new WECLU( 'storage' );
$storage -> create( 'st1', 'RDS' );
/*$res = $storage -> delete_index( 'st1', array( 'sex' ) );
print_r($res);
$res = $storage -> create_index( 'st1', array( 'sex', 'name.tai' ) );print_r($res);exit;*/
//print_r($storage -> create_index( 'st1', array( 'sex' ) ));
$t1 = microtime(true);
$i = 1003;
//$storage -> insert('st1', array( 'name' => array( 'tai' => 'zjr' . $i ), 'sex' => 'man1231' . $i ) );
//for ( $i=1; $i<=10000; $i++ ) {
	//$storage -> insert('st1', array( 'name' => array( 'tai' => 'zjr' . $i ), 'sex' => 'man1231' . $i ) );
//}
//exit;
$res = $storage -> select( 'st1', 'name.tai,_id', array( '_id' => 123 ), array( 'name.tai' => 'desc' ), 1, 20 );
print_r($res);
$t2 = microtime(true);
echo $t2 - $t1; echo "\n";
exit;

