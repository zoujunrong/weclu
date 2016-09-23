<?php

//计算ord的值
function weclu_sum_ord( $str ) {
    $sum = 0;
    if ( ! is_numeric( $str ) && strlen( $str ) ) {
        for ($i=0; $i<strlen($str);$i++) {
            $chr = substr($str, $i, 1);
            $sum += ord($chr);
        }
    } else $sum = intval( $str );
    return $sum;
}

function weclu_mkdirs( $path, $i=0 ) {
    $path_out = preg_replace('/[^\/.]+\/?$/', '', $path);
    if (!is_dir($path_out)) {
        if($i<50){
            weclu_mkdirs($path_out,++$i);
        }
    }
    mkdir($path);
}

function weclu_hash( $str ) {
	$md5_str = md5($str);
	return array( base_convert( substr( $md5_str, 0, 5 ), 16, 10 ) * WECLU_HASH_BLOCK_SIZE, base_convert( substr( $md5_str, 5, 5 ), 16, 10 ) );
}

function weclu_array_orderby( $args ) {
// 	$args = func_get_args();
	$data = array_shift( $args );
	foreach ( $args as $n => $field ) {
		if ( is_string( $field ) ) {
			$tmp = array();
			foreach ( $data as $key => $row )
				$tmp[$key] = $row[$field];
			$args[$n] = $tmp;
		}
	}
	$args[] = &$data;
	call_user_func_array( 'array_multisort', $args );
	return array_pop( $args );
}

function weclu_filter_filed( $filed ) {
	$retFiled = array();
	$filed = is_array( $filed ) ? $filed : explode( ',', $filed );
	if ( ! empty( $filed ) ) {
		foreach ( $filed as $v ) {
			$retFiled[trim($v)] = 1;
		}
	}
	return $retFiled;
}

function weclu_array_slice( &$arr, $offset ) {
	$ret = array();
	if ( ! empty( $arr ) ) {
		$i = 0;
		$count = count( $arr );
		foreach ( $arr as $k => $v ) {
			if ( $i >= $offset ) {
				$ret[$k] = $v;
				unset( $arr[$k] );
			}
			$i++;
		}
	}
	return $ret;
}

function weclu_deldir( $dir ) {
    $dh = opendir( $dir );
    while ( $file = readdir( $dh ) ) {
        if( $file != "." && $file != ".." ) {
            $fullpath = $dir . "/" . $file;
            if( ! is_dir( $fullpath ) ) {
                unlink( $fullpath );
            } else {
                weclu_deldir( $fullpath );
            }
        }
    }
    closedir( $dh );
    if( rmdir( $dir ) ) {
        return true;
    } else {
        return false;
    }
}
