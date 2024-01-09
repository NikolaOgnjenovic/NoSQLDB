use std::rc::Rc;
use std::cell::RefCell;
use crate::dll_node::{ Entry, Node };


#[derive(Debug)]
pub(crate) struct DoublyLinkedList {
    pub(crate) head: Link,
    pub(crate) tail: Link,
    pub(crate) size: usize,

}

impl DoublyLinkedList {

    pub(crate) fn new() -> Self {
        Self {
            head: None,
            tail: None,
            size:0,
        }
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    fn push_tail(&mut self, el: Entry) {
        let node = Rc::new(RefCell::new(Node::new(el)));
        if let Some(prev_tail) = self.tail.take() {

            prev_tail.borrow_mut().prev = Some(Rc::clone(&node));
            node.borrow_mut().next = Some(prev_tail);
            self.tail = Some(node);

        } else {

            self.head = Some(Rc::clone(&node));
            self.tail = Some(node);
        }

        self.size += 1;
    }

    pub(crate) fn push_head(&mut self, el: Entry) {

        let node = Rc::new(RefCell::new(Node::new(el)));
        if let Some(prev_head) = self.head.take() {

            prev_head.borrow_mut().next = Some(Rc::clone(&node));
            node.borrow_mut().prev = Some(prev_head);
            self.head = Some(node);

        } else {
            self.head = Some(Rc::clone(&node));
            self.tail = Some(node);
        }

        self.size += 1;
    }

    pub(crate) fn pop_tail(&mut self) -> Link {

        if self.is_empty() {
            return None;
        }

        if let Some(prev_tail) = self.tail.take() {

            if prev_tail.borrow().next.is_none() {
                self.tail = None;
                self.head = None;
                self.size -= 1;
                return Some(prev_tail);

            } else {
                let new_last= prev_tail.as_ref().borrow().next.clone().unwrap();
                Rc::clone(&new_last).borrow_mut().prev = None;
                self.tail = Some(Rc::clone(&new_last));
                self.size -= 1;
                return Some(prev_tail);
            }
        }

        None
    }

    fn pop_head(&mut self) -> Link {

        if self.is_empty() {
            return None;
        }
        if let Some(prev_head) = self.head.take() {
            if prev_head.borrow().prev.is_none() {

                self.tail = None;
                self.head = None;
                self.size -= 1;
                return Some(prev_head);

            } else {

                let new_first = prev_head.as_ref().borrow().prev.clone().unwrap();
                Rc::clone(&new_first).borrow_mut().next = None;
                self.head = Some(Rc::clone(&new_first));
                self.size -= 1;
                return Some(prev_head);
            }
        }

        None
    }

    pub fn peak_head(&self) -> Link {
        if !self.is_empty() {
            return self.head.clone();
        }
        None
    }

    pub fn peak_tail(&self) -> Link {
        if !self.is_empty() {
            return self.tail.clone();
        }
        None
    }
}

impl Iterator for DoublyLinkedList {
    type Item = Link;

    fn next(&mut self) -> Option<Self::Item>{

        while !self.is_empty() {
            return Some(self.pop_head());
        }
        None
    }
}

impl Drop for DoublyLinkedList {
    fn drop(&mut self) {
        while self.pop_head().is_some() {}
    }
}

pub(crate) type Link = Option<Rc<RefCell<Node>>>;