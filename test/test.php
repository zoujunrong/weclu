<?php
include_once './index.class.php';
include_once './storage.class.php';
$storage = new Storage( 'storage', 'stg' );
$t1 = microtime(true);
/*$data = array( 'name' => 'zoujunrong4zoujun100002', 'sex' => 'man100002' );
	$id = $storage -> insert_main( $data );
	echo $id;
	exit;
*/for( $i=1; $i<=100000; $i++ ){
	$data = array( 'name' => 'zoujunrong4zoujun' . $i, 'sex' => 'man' . $i );
	$id = $storage -> insert_main( $data );
	//$data = $storage -> fetch_main( $i );
	//if ( empty( $data ) ) echo $i . "\n";
}
//$storage -> update_main( $id, $data );
print_r($storage -> fetch_main( $id ) );
//$storage -> delete_main( $id );
//print_r($storage -> fetch_main( $id ) );
$t2 = microtime(true);
echo $t2 - $t1; echo "\n";
exit;
$node = array( 'a' => 1 );
//echo base_convert('16e', 32, 10);exit;
$btree = new BtreeIndex( 'index', 'test' );
$t1 = microtime(true);
//$btree -> add_btree_index( 'intrest', '123345zjr12123345', 10 );
$btree -> update_btree_index( 'intrest', '348zjr12348', '1348zjr121348', 349 );
print_r($btree -> get_btree_index( 'intrest', '1348zjr121348' ));
exit;
//构造100万数据
for( $i=0; $i<1000; $i++ ) {
	$btree -> add_btree_index( 'intrest', $i . 'zjr12' . $i, $i+1 );
	$data = $btree -> get_btree_index( 'intrest', $i . 'zjr12' . $i );
	if ( empty( $data ) ) echo $i . 'zjr12' . $i . "\n";
}
$t2 = microtime(true);
echo $t2 - $t1; echo "\n";
exit;
$btree -> add_btree_index( 'name', '12zjr12', 3 );
print_r( $btree -> get_btree_index( 'name', '12zjr12' ) );exit;
//print_r(seek_btree_node_key(array('a' => 1, 'f' => 1), 'g'));exit;
//$btree -> add_btree_index( 'name', 'shaofei', 2893 );
print_r( $btree -> get_btree_index( 'name', array(array('==' => 'zjr9'), array('==' => 'zjr8'), array('==' => 'zjr12'),array('>=' => 'zjr1', '<=' => 'zjr12' )) ) );
//print_r($btree -> get_btree_index( 'name', array(array('like' => 'zjr12%')) ));
for( $i=0; $i<10000; $i++ ) {
	//$btree -> add_btree_index( 'name', 'zjr' . $i, $i+1 );
}
$t2 = microtime(true);
echo $t2 - $t1; echo "\n";

function seek_btree_node_key( $node, $key ) {
	    $prev = $cur = $next = '';
	    foreach ( $node as $k => $p ) {
	        if( $cur >= $key && $k > $key ) {
                $next = $k;
                break;
	        }
	        $prev = $cur;
	        $cur = $k;
	    }
	    return array( $prev, $cur, $next );
	}