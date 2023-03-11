use std::collections::HashMap;

use crate::types::{Arc, Mutex};

use super::types::FrameId;

struct Node {
    next: Option<FrameId>,
    previous: Option<FrameId>,
}

struct UnsafeLRU {
    map: HashMap<FrameId, Node>,
    first: Option<FrameId>,
    last: Option<FrameId>,
}

#[derive(Clone)]
pub(super) struct LRU(Arc<Mutex<UnsafeLRU>>);

impl LRU {
    pub fn new() -> Self {
        LRU(Arc::new(Mutex::new(UnsafeLRU { map: HashMap::new(), first: None, last: None })))
    }

    pub fn add(&mut self, frame_id: FrameId) {
        let mut lru = self.0.lock().unwrap();

        if !lru.map.contains_key(&frame_id) {
            let new_node = Node { previous: None, next: lru.first };
            lru.map.insert(frame_id, new_node);

            if let Some(current_first) = lru.first {
                lru.map.get_mut(&current_first).unwrap().previous = Some(frame_id);
            } else {
                lru.last = Some(frame_id);
            }
            lru.first = Some(frame_id);
        }
    }

    pub fn remove(&mut self, frame_id: FrameId) {
        let mut lru = self.0.lock().unwrap();

        if let Some(removed_node) = lru.map.remove(&frame_id) {
            if let Some(previous) = removed_node.previous {
                lru.map.get_mut(&previous).unwrap().next = removed_node.next;
            }
            if let Some(next) = removed_node.next {
                lru.map.get_mut(&next).unwrap().previous = removed_node.previous;
            }
            if let Some(first) = lru.first {
                if first == frame_id {
                    lru.first = removed_node.next;
                }
            }
            if let Some(last) = lru.last {
                if last == frame_id {
                    lru.last = removed_node.previous;
                }
            }
        }
    }

    pub fn remove_last(&mut self) -> Option<FrameId> {
        let mut lru = self.0.lock().unwrap();

        lru.last.take().map(|frame_id| {
            if let Some(removed_node) = lru.map.remove(&frame_id) {
                lru.last = removed_node.previous;
                if let Some(previous) = removed_node.previous {
                    lru.map.get_mut(&previous).unwrap().next.take();
                }
                if let Some(first) = lru.first {
                    if first == frame_id {
                        lru.first.take();
                    }
                }
            }
            frame_id
        })
    }

    fn size(&self) -> usize {
        self.0.lock().unwrap().map.len()
    }
}

#[cfg(test)]
mod test {
    use crate::types::check_random;
    use crate::types::thread;

    use super::LRU;

    #[test]
    fn should_work() {
        let mut lru = LRU::new();

        lru.add(1);
        lru.add(2);
        lru.add(3);
        lru.add(4);
        lru.add(5);
        lru.add(6);
        assert_eq!(lru.size(), 6);

        assert_eq!(lru.remove_last(), Some(1));
        assert_eq!(lru.remove_last(), Some(2));
        assert_eq!(lru.remove_last(), Some(3));

        lru.remove(3);
        lru.remove(4);
        assert_eq!(lru.size(), 2);

        lru.add(4);

        assert_eq!(lru.remove_last(), Some(5));
        assert_eq!(lru.remove_last(), Some(6));
        assert_eq!(lru.remove_last(), Some(4));
        assert_eq!(lru.size(), 0);
    }

    #[test]
    fn should_remove_from_empty_lru() {
        let mut lru = LRU::new();

        lru.remove(0);
        assert_eq!(lru.size(), 0)
    }

    #[test]
    fn should_find_no_last_in_empty_lru() {
        let mut lru = LRU::new();

        assert_eq!(lru.remove_last(), None)
    }

    #[test]
    fn should_add_remove_when_interleave() {
        let mut lru = LRU::new();
        lru.add(1);
        lru.add(2);
        lru.remove(2);
        lru.remove(1);

        assert_eq!(lru.size(), 0)
    }

    #[test]
    fn should_work_concurrently() {
        check_random(
            || {
                let lru = LRU::new();

                let mut lru1 = lru.clone();
                let handle1 = thread::spawn(move || {
                    for _ in 0..100 {
                        lru1.add(1);
                        lru1.remove(1);
                    }
                });
                let mut lru2 = lru.clone();
                let handle2 = thread::spawn(move || {
                    for _ in 0..100 {
                        lru2.add(2);
                        lru2.remove(2);
                    }
                });
                handle1.join().unwrap();
                handle2.join().unwrap();
                assert_eq!(lru.size(), 0)
            },
            100,
        );
    }
}
