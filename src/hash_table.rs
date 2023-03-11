use std::cell::RefCell;
use std::rc::Rc;
use std::usize;

#[derive(Debug, Clone, Copy)]
struct Tuple {
    key: u32,
    value: u32,
}

#[derive(Debug)]
struct Bucket {
    depth: u32,
    tuples: Vec<Tuple>,
}

impl Bucket {
    fn new() -> Self {
        Bucket { depth: 1, tuples: Vec::new() }
    }

    fn get(&self, key: u32) -> Vec<u32> {
        self.tuples.iter().filter(|tuple| tuple.key == key).map(|tuple| tuple.value).collect()
    }

    fn is_full(&self, bucket_size_limit: usize) -> bool {
        self.tuples.len() >= bucket_size_limit
    }

    fn put(&mut self, key: u32, value: u32) {
        self.tuples.push(Tuple { key, value })
    }
}

#[derive(Clone, Debug)]
struct HashTable {
    depth: u32,
    directories: Vec<Rc<RefCell<Bucket>>>,
    bucket_size_limit: usize,
}

impl HashTable {
    fn new(bucket_size_limit: usize) -> Self {
        let mut buckets = Vec::new();
        buckets.resize_with(2, || Rc::new(RefCell::new(Bucket::new())));
        HashTable { depth: 1, directories: buckets, bucket_size_limit }
    }

    fn get(&self, key: u32) -> Vec<u32> {
        self.directories[self.idx(key) as usize].borrow().get(key)
    }

    fn put(&mut self, key: u32, value: u32) {
        let idx = self.idx(key);
        let bucket = self.directories[idx as usize].borrow();
        let is_full = bucket.is_full(self.bucket_size_limit);
        let needs_expansion = bucket.depth == self.depth;
        drop(bucket);

        if is_full {
            if needs_expansion {
                self.expand_directories();
            }
            self.split_bucket(idx);
        }

        self.put_simple(key, value);
    }

    fn put_simple(&mut self, key: u32, value: u32) {
        self.directories[self.idx(key) as usize].borrow_mut().put(key, value)
    }

    fn expand_directories(&mut self) {
        let mut new_buckets: Vec<Rc<RefCell<Bucket>>> = Vec::new();
        let new_size = u32::pow(2, self.depth + 1);
        new_buckets.reserve(new_size as usize);

        for new_idx in 0..new_size {
            let idx = HashTable::mask(new_idx, self.depth);
            new_buckets.push(self.directories[idx as usize].clone());
        }

        self.directories = new_buckets;
        self.depth += 1;
    }

    fn split_bucket(&mut self, bucket_idx: u32) {
        let mut bucket = self.directories[bucket_idx as usize].borrow_mut();
        bucket.depth += 1;
        let (keep, remove) =
            bucket.tuples.iter().partition(|tuple| HashTable::mask(tuple.key, bucket.depth) == bucket_idx);
        bucket.tuples = keep;

        let new_idx = HashTable::split_image_idx(bucket_idx, bucket.depth);
        let mut new_idx_bucket = Bucket::new();
        new_idx_bucket.depth = bucket.depth;
        new_idx_bucket.tuples = remove;

        drop(bucket);
        self.directories[new_idx as usize] = Rc::new(RefCell::new(new_idx_bucket));
    }

    fn idx(&self, key: u32) -> u32 {
        HashTable::mask(key, self.depth)
    }

    fn mask(bucket_idx: u32, depth: u32) -> u32 {
        let mask = u32::checked_shl(1, depth).unwrap_or(0).wrapping_sub(1);
        bucket_idx & mask
    }

    fn split_image_idx(bucket_idx: u32, depth: u32) -> u32 {
        u32::checked_shl(1, depth - 1).map(|a| a ^ bucket_idx).unwrap_or(bucket_idx)
    }
}

#[cfg(test)]
mod test {
    use super::HashTable;

    #[test]
    fn should_find_least_significant_bits() {
        assert_eq!(HashTable::mask(0b1111, 2), 0b11);
        assert_eq!(HashTable::mask(0b11, 32), 0b11);
        assert_eq!(HashTable::mask(u32::MAX, 32), u32::MAX);
        assert_eq!(HashTable::mask(0b1010, 2), 0b10);
        assert_eq!(HashTable::mask(0, 2), 0);
        assert_eq!(HashTable::mask(0b0, 2), 0);
    }

    #[test]
    fn should_get_split_image_idx() {
        assert_eq!(HashTable::split_image_idx(0, 1), 1);
        assert_eq!(HashTable::split_image_idx(0b1, 1), 0);
        assert_eq!(HashTable::split_image_idx(0b11, 2), 0b01);
        assert_eq!(HashTable::split_image_idx(0b1111, 4), 0b0111);
        assert_eq!(HashTable::split_image_idx(0b101, 3), 0b0001);
        assert_eq!(HashTable::split_image_idx(0b0001, 3), 0b0101);
    }

    #[test]
    fn should_not_find_non_existent_value() {
        let table = HashTable::new(3);

        assert!(table.get(10).is_empty())
    }

    #[test]
    fn should_put_get() {
        let mut table = HashTable::new(3);

        table.put(10, 11);
        assert_eq!(table.get(10), vec![11])
    }

    #[test]
    fn should_put_get_multiple_values() {
        let mut table = HashTable::new(3);

        table.put(10, 11);
        table.put(10, 12);
        assert_eq!(table.get(10), vec![11, 12])
    }

    #[test]
    fn should_put_get_multiple() {
        let mut table = HashTable::new(3);

        table.put(16, 16);
        assert_eq!(table.get(16), vec![16]);

        table.put(4, 4);
        assert_eq!(table.get(4), vec![4]);

        table.put(6, 6);
        assert_eq!(table.get(6), vec![6]);

        table.put(22, 22);
        assert_eq!(table.get(22), vec![22]);

        table.put(24, 24);
        assert_eq!(table.get(24), vec![24]);

        table.put(10, 10);
        assert_eq!(table.get(10), vec![10]);

        table.put(31, 31);
        assert_eq!(table.get(31), vec![31]);

        table.put(7, 7);
        assert_eq!(table.get(7), vec![7]);

        table.put(9, 9);
        assert_eq!(table.get(9), vec![9]);

        table.put(20, 20);
        assert_eq!(table.get(20), vec![20]);

        table.put(26, 26);
        assert_eq!(table.get(26), vec![26]);

        assert_eq!(table.get(16), vec![16]);
        assert_eq!(table.get(4), vec![4]);
        assert_eq!(table.get(6), vec![6]);
        assert_eq!(table.get(22), vec![22]);
        assert_eq!(table.get(24), vec![24]);
        assert_eq!(table.get(10), vec![10]);
        assert_eq!(table.get(31), vec![31]);
        assert_eq!(table.get(7), vec![7]);
        assert_eq!(table.get(9), vec![9]);
        assert_eq!(table.get(20), vec![20]);
        assert_eq!(table.get(26), vec![26]);
    }

    #[test]
    fn should_put_get_many() {
        let mut table = HashTable::new(491);

        for i in 1..10000 {
            table.put(i, i);
            table.put(i, 2 * i);
            assert_eq!(table.get(i), vec![i, 2 * i]);
        }

        for i in 1..10000 {
            assert_eq!(table.get(i), vec![i, 2 * i]);
        }
    }
}
