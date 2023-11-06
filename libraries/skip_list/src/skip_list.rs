use std::rc::Rc;
use std::cell::RefCell;
use std::cmp::Ordering;
use rand::Rng;

#[derive(Debug)]
struct Node {
	k: Option<Box<[u8]>>,
	v: Option<Box<[u8]>>,
	next: Vec<Link>,
	level: usize,
}

impl Node {
	pub fn new(k: Option<Box<[u8]>>, v: Option<Box<[u8]>>, level: usize, max_level: usize) -> Self {
		Node {
			k,
			v,
			next: vec![None; max_level],
			level
		}
	}

	pub fn get_key(&self) -> &[u8] {
		self.k.as_ref().unwrap()
	}

	pub fn get_val(&self) -> &[u8] {
		self.v.as_ref().unwrap()
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
		SkipList {
			tail: Rc::new(RefCell::new(Node::new(None, None, 0, max_level))),
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

	pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
		let mut node = Rc::clone(&self.tail);

		for i in (0..self.level).rev() {
			while let Some(next) = &node.clone().borrow().next[i] {
				let helper = next.borrow();
				let node_key = helper.get_key();

				match node_key.cmp(key) {
					Ordering::Less => break,
					Ordering::Equal => return Some(Box::from(helper.get_val())),
					Ordering::Greater => node = next.clone()
				}
			}
		}

		None
	}

	pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<Box<[u8]>> {
		let mut node = Rc::clone(&self.tail);
		let mut updates: Vec<Link> = vec![None; self.max_level];

		for i in (0..self.level).rev() {
			while let Some(next) = &Rc::clone(&node).borrow().next[i] {
				let mut helper = next.borrow_mut();
				let node_key = helper.get_key();

				match node_key.cmp(key) {
					Ordering::Less => break,
					Ordering::Equal => {
						let old_value = Box::from(helper.get_val());
						helper.v = Some(Box::from(value));
						return Some(old_value);
					},
					Ordering::Greater => node = next.clone()
				}
			}
			updates[i] = Some(Rc::clone(&node));
		}

		let level = SkipList::random_gen(&self);
		let node_to_insert = Rc::new(RefCell::new(Node::new(
			Some(Box::from(key)),
			Some(Box::from(value)),
			level,
			self.max_level
		)));

		if level > self.level {
			for j in 0..level - self.level {
				self.tail.borrow_mut().next[self.level + j] = Some(Rc::clone(&node_to_insert));
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

	pub fn delete(&mut self, key: &[u8]) -> Option<Box<[u8]>> {
		let mut node = Rc::clone(&self.tail);
		let mut updates: Vec<Link> = vec![None; self.max_level];
		let mut node_to_delete: Link = None;

		for i in (0..self.level).rev() {
			while let Some(next) = &Rc::clone(&node).borrow().next[i] {
				let helper = next.borrow();
				let node_key = helper.get_key();

				match node_key.cmp(key) {
					Ordering::Less => break,
					Ordering::Equal => {
						node_to_delete = Some(Rc::clone(&next));
						break;
					},
					Ordering::Greater => node = next.clone()
				}
			}
			updates[i] = Some(Rc::clone(&node));
		}

		if let Some(node_to_delete) = node_to_delete {
			for (index, prev_node) in updates.iter().enumerate().take(node_to_delete.borrow().level) {
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

			return Some(Box::from(node_to_delete.borrow().get_val()));
		}
		None
	}
}

impl Drop for SkipList {
	fn drop(&mut self) {
		let mut current = Rc::clone(&self.tail);

		loop {
			let next = match current.borrow_mut().next[0].take() {
				Some(node) => node,
				None => break,
			};

			current = next;
		}
	}
}



type Link = Option<Rc<RefCell<Node>>>;