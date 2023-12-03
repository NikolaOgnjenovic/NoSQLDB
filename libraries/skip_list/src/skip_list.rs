use std::rc::Rc;
use std::cell::RefCell;
use std::cmp::Ordering;
use rand::Rng;
use segment_elements::{MemoryEntry, TimeStamp};
use crate::skip_list_node::{Node, Link};
use crate::skip_list_iterator::SkipListIterator;
use bloom_filter::BloomFilter;

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

	pub fn delete_permanent(&mut self, key: &[u8]) -> Option<Box<[u8]>> {
		let mut node = Rc::clone(&self.tail);
		let mut updates: Vec<Link> = vec![None; self.max_level];
		let mut node_to_delete: Link = None;

		for i in (0..self.level).rev() {
			while let Some(next) = &Rc::clone(&node).borrow().next[i] {
				let helper = next.borrow();
				let node_key = helper.get_key();

				match key.cmp(node_key) {
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

			return Some(node_to_delete.borrow().get_val().get_value());
		}

		None
	}
	pub fn iter(&self) -> SkipListIterator {
		SkipListIterator{
			current: Some(Rc::clone(&self.tail)),
		}
	}
}

impl segment_elements::SegmentTrait for SkipList {
	fn insert(&mut self, key: &[u8], value: &[u8], time_stamp: TimeStamp) -> bool {
		let mut node = Rc::clone(&self.tail);
		let mut updates: Vec<Link> = vec![None; self.max_level];

		for i in (0..self.level).rev() {
			while let Some(next) = &Rc::clone(&node).borrow().next[i] {
				let mut helper = next.borrow_mut();
				let node_key = helper.get_key();

				match key.cmp(node_key) {
					Ordering::Less => break,
					Ordering::Equal => {
						helper.value = Some(MemoryEntry::from(value, false, time_stamp.get_time()));
						return false;
					}
					Ordering::Greater => node = next.clone()
				}
			}
			updates[i] = Some(Rc::clone(&node));
		}

		let level = SkipList::random_gen(&self);
		let node_to_insert = Rc::new(RefCell::new(Node::new(
			Some(Box::from(key)),
			Some(MemoryEntry::from(value, false, time_stamp.get_time())),
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
		true
	}

	fn delete(&mut self, key: &[u8], time_stamp: TimeStamp) -> bool {
		let mut node = Rc::clone(&self.tail);

		for i in (0..self.level).rev() {
			while let Some(next) = &node.clone().borrow().next[i] {
				let mut helper = next.borrow_mut();
				let node_key = helper.get_key();

				match key.cmp(node_key) {
					Ordering::Less => break,
					Ordering::Equal => {
						helper.value.as_mut().unwrap().set_timestamp(time_stamp);
						helper.value.as_mut().unwrap().set_tombstone(true);

						return true;
					},
					Ordering::Greater => node = next.clone()
				}
			}
		}

		false
	}

	fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
		let mut node = Rc::clone(&self.tail);

		for i in (0..self.level).rev() {
			while let Some(next) = &node.clone().borrow().next[i] {
				let helper = next.borrow();
				let node_key = helper.get_key();

				match key.cmp(node_key) {
					Ordering::Less => break,
					Ordering::Equal => {
						return if !helper.get_val().get_tombstone() {
							Some(helper.get_val().get_value())
						} else {
							None
						};
					},
					Ordering::Greater => node = next.clone()
				}
			}
		}

		None
	}
	///Function that serializes skip list and creates data_block, index file and filter for SSTable
	fn serialize(&self) -> Box<[u8]> {
		//todo! how to make index summary?
		let mut ss_table_bytes = vec![];
		let mut data_bytes:Vec<u8> = vec![];
		let mut index_bytes = vec![];
		let mut offset = 0;
		let mut filter = BloomFilter::new(0.01, self.length);

		for node in self.iter() {

			let borrowed = node.borrow();
			let key = borrowed.get_key();
			let entry = borrowed.get_val();
			let entry_bytes = entry.serialize(key);
			data_bytes.extend(entry_bytes.iter());

			//index structure contains key_len(8 bytes), key and offset in data block(8 bytes)
			index_bytes.extend(usize::to_ne_bytes(key.len()));
			index_bytes.extend(key);
			index_bytes.extend(usize::to_ne_bytes(offset));

			filter.add(key);

			offset += entry_bytes.len();
		}

		let filter_bytes = filter.serialize();

		ss_table_bytes.extend(usize::to_ne_bytes(data_bytes.len()));
		ss_table_bytes.extend(data_bytes);
		ss_table_bytes.extend(usize::to_ne_bytes(index_bytes.len()));
		ss_table_bytes.extend(index_bytes);
		ss_table_bytes.extend(usize::to_ne_bytes(filter_bytes.len()));
		ss_table_bytes.extend(filter_bytes.iter());
		
		ss_table_bytes.into_boxed_slice()
	}

	fn empty(&mut self) {
		self.tail = Rc::new(RefCell::new(Node::new(None, None, 0, self.max_level)));
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


