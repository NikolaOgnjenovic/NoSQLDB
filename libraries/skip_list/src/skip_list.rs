use std::rc::Rc;
use std::cell:: { RefCell, Ref };
use rand::Rng;


#[derive(Debug)]
struct Node {
	k: [u8; 8],
	v: [u8; 8],
	next: Vec<Link>,
	level: usize,

}

impl Node {
	pub fn new(k: [u8; 8], v: [u8; 8], l: usize, max_level: usize) -> Self {
		Node {
			k,
			v,
			next: vec![None; max_level],
			level: l,
		}
	}

	pub fn get_key(&self) -> &[u8; 8] {
		&self.k
	}

	pub fn get_val(&self) -> &[u8; 8] {
		&self.v
	}

	pub fn set_level(&mut self, lvl: usize) {
		self.level = lvl;
	}

	pub fn get_level(&self) -> usize {
		self.level
	}

	pub fn set_value(&mut self, val: [u8;8]) {
		self.v = val;
	}
}


pub struct SkipList {
	tail: Rc<RefCell<Node>>,
	level: usize,
	max_level: usize,
	length: usize,
}

impl SkipList {

	pub fn new(max_level: usize) -> Self {
		SkipList{
			tail: Rc::new(RefCell::new(Node::new([0,0,0,0,0,0,0,0], [0,0,0,0,0,0,0,0], 0, max_level))),
			level: 0,
			max_level,
			length: 0,
		}
	}

	fn random_gen(&self) -> usize {
		let mut rng = rand::thread_rng();
		let mut level = 1;
		while rng.gen_range(0..=1) == 1 && level < self.max_level {
			level += 1;
		}
	
		level
	}

	pub fn get(&self, k: &[u8;8]) -> Option<[u8;8]> {

		let mut node = Rc::clone(&self.tail);
		
		for i in (0..self.level).rev() {

			while let Some(next) = &node.clone().borrow().next[i] {
				let helper = next.borrow();
				let key = helper.get_key();
				if compare_bytes(key, k) == 0 {
					return Some(helper.get_val().clone());
				}
				if compare_bytes(key, k) == 2 {
					node = next.clone();
				} else {
					break;
				}
			}

		}

		None
	}

	pub fn insert(&mut self, k: [u8;8], mut v: [u8;8]) -> Option<[u8;8]> {

		let mut node = Rc::clone(&self.tail);
		let mut updates: Vec<Link> = vec![None; self.max_level];

		for i in (0..self.level).rev() {

			while let Some(next) = &Rc::clone(&node).borrow().next[i] {
				let mut helper = next.borrow_mut();
				let key = helper.get_key();
				let value = helper.get_val().clone();

				if compare_bytes(key, &k) == 0 {
					helper.v = v;
					return Some(value);
				}
				if compare_bytes(key, &k) == 2 {
					node = next.clone();
				} else {
					break;
				}
			}
			updates[i] = Some(Rc::clone(&node));
		}

		let level = SkipList::random_gen(&self);
		let node_to_insert = Rc::new(RefCell::new(Node::new(k, v, level, self.max_level)));

		if level > self.level {
			for j in 0..level-self.level {
				self.tail.borrow_mut().next[self.level+j] = Some(Rc::clone(&node_to_insert));
			}
			self.level = level;
		}

		for (index, prev_node) in updates.iter().enumerate().take(level) {
			if let Some(prev_node) = prev_node {
				let borrowed_prev = &mut prev_node.borrow_mut();
				let next_node = &borrowed_prev.next[index];
				if let Some(next_node) = next_node {
					node_to_insert.borrow_mut().next[index] = Some(Rc::clone(&next_node));
				}
				borrowed_prev.next[index] = Some(Rc::clone(&node_to_insert));
			}
		}

		self.length += 1;

		None
	}
	

	pub fn delete(&mut self, k: &[u8;8]) -> Option<[u8;8]> {
		
		let mut node = Rc::clone(&self.tail);
		let mut updates: Vec<Link> = vec![None; self.max_level];
		let mut node_to_delete: Link = None;

		for i in (0..self.level).rev() {

			while let Some(next) = &Rc::clone(&node).borrow().next[i] {
				let helper = next.borrow();
				let key = helper.get_key();
				
				if compare_bytes(key, k) == 0 {
					node_to_delete = Some(Rc::clone(&next));
				}
				if compare_bytes(key, k) == 2 {
					node = next.clone();
				} else {
					break;
				}
			}
			updates[i] = Some(Rc::clone(&node));
		}

		if let Some(node_to_delete) = node_to_delete {
			for (index, prev_node) in updates.iter().enumerate().take(node_to_delete.borrow().get_level()) {
				if let Some(prev_node) = prev_node {
					let next = &node_to_delete.borrow().next[index];
					if next.is_some() {
						prev_node.borrow_mut().next[index] = Some(Rc::clone(&next.as_ref().unwrap()));
					} else {
						prev_node.borrow_mut().next[index] = None;
					}
				}
			} 

			self.length -= 1;

			return Some(*node_to_delete.borrow().get_val());
		}
		None
	}

	//TODO.. serialize & deserialize functions

} 

//TODO bolje iz [u8;8] dobiti usize::from_ne_bytes ali otom potom
fn compare_bytes(first: &[u8; 8], second: &[u8; 8]) -> usize {

	for i in (0..8).rev() {
		if first[i] > second[i] {
			return 1;
		} else if second[i] > first[i] {
			return 2;
		} else {
			continue;
		}
	}
	0
}


type Link = Option<Rc<RefCell<Node>>>;