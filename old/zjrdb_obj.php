<?php
/**
 * @description 数据对象操作类
 * @author zjr
 * @create time 2016/3/30
 * @V2.0
 * @update time 2016/3/31
 * @description
 */
class ZJRDBOBJ{
	private $result		= array();
	private $_id		= NULL;
	private $root		= NULL;
	public function __construct($dom = array(), $root=NULL) {
		$this->init($dom, $root);
	}

	public function init($dbDom, &$root=NULL) {
		if (!empty($dbDom)) {
			$this->result = $dbDom;
		}
		if (!empty($root)) {
			$this->root = $root;
		}  else {
			$this->root = &$this;
		}
		return $this;
	}

	/**
	 * 数据对象
	 */
	public function find($where) {
		$findRes = array();
		if (!empty($where) && !empty($this->result)) {
			foreach ($this->result as $key => $value) {
				$this->compareWhere($key, $value, $where);
			}
		}
		return new ZJRDBOBJ($findRes, $this->root);
	}

	private function compareWhere($key, $value, &$where) {
		if (!empty($value)) {
			if ($value == $where) {
				$this->result[$key] = $value;
			} elseif (is_array($value)) {
				foreach ($value as $k => $v) {
					$this->compareWhere($key, $v, $where);
				}
			}
		}
	}

	public function each($method) {
		foreach ($this->result as $key => $value) {
			$method($key, new ZJRDBOBJ($value));
		}
	}

	public function count() {
		return count($this->result);
	}

	public function relations() {
		$this->
		return $this;
	}

	public function root(){
		return $this->root;
	}

	public function parent() {
		return $this;
	}

	public function child() {
		return $this;
	}

	public function change() {
		return $this;
	}

	public function append() {
		return $this;
	}

	public function remove() {
		return $this;
	}

	public function result() {
		return $this->result;
	}

	public function clear() {
		return $this;
	}

	public function submit() {
		if (!empty($result)) {

		}
		if (!empty($document)) {
			if (isset($document['_id'])) {
				$this->update();
			} else {
				$this->insert();
			}
		}
		return true;
	}
	
}
