use std::cmp::max;

// https://www.cs.umd.edu/class/fall2019/cmsc420-0201/Lects/lect06-aa.pdf
// https://people.ksp.sk/~kuko/gnarley-trees/AAtree.html
#[derive(Clone)]
struct Node {
    value: u32,
    level: u32,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    pub fn new_leaf(value: u32) -> Node {
        Node::new(value, 1)
    }
    pub fn new(value: u32, level: u32) -> Node {
        Node { value, level, left: None, right: None }
    }

    pub fn put(&self, value: u32) -> Node {
        fn inner(current: &Node, value: u32) -> Node {
            let inserted_node = if current.value == value {
                current.clone()
            } else if value < current.value {
                Node {
                    value: current.value,
                    level: current.level,
                    left: Some(Box::new(
                        current.left.as_ref().map_or_else(|| Node::new(value, current.level), |n| inner(n, value)),
                    )),
                    right: current.right.clone(),
                }
            } else {
                Node {
                    value: current.value,
                    level: current.level,
                    left: current.left.clone(),
                    right: Some(Box::new(
                        current.right.as_ref().map_or_else(|| Node::new(value, current.level), |n| inner(n, value)),
                    )),
                }
            };

            split(skew(inserted_node))
        }

        fn skew(mut node: Node) -> Node {
            match node.left.take() {
                None => node,
                Some(mut left) => {
                    if left.level == node.level {
                        node.left = left.right.take();
                        left.right = Some(Box::new(node));
                        *left
                    } else {
                        node.left = Some(left);
                        node
                    }
                }
            }
        }

        fn split(mut node: Node) -> Node {
            match node.right.take() {
                None => node,
                Some(mut right) => match right.right.as_ref() {
                    None => {
                        node.right = Some(right);
                        node
                    }
                    Some(right_right) => {
                        if node.level == right.level && right.level == right_right.level {
                            node.right = right.left.take();
                            right.left = Some(Box::new(node));
                            right.level += 1;
                            *right
                        } else {
                            node.right = Some(right);
                            node
                        }
                    }
                },
            }
        }

        inner(self, value)
    }

    pub fn collect_to_vec(&self) -> (Vec<u32>, Vec<u32>) {
        let mut values = vec![];
        let mut levels = vec![];
        Node::traverse(self, &mut |node| {
            values.push(node.value);
            levels.push(node.level)
        });
        (values, levels)
    }

    fn traverse<F>(node: &Node, f: &mut F)
    where
        F: FnMut(&Node),
    {
        if let Some(left) = node.left.as_ref() {
            Node::traverse(left, f);
        }
        f(node);
        if let Some(right) = node.right.as_ref() {
            Node::traverse(right, f);
        }
    }

    pub fn depth(&self) -> u32 {
        match (self.left.as_ref(), self.right.as_ref()) {
            (Some(left), Some(right)) => max(left.depth(), right.depth()) + 1,
            (Some(left), None) => left.depth() + 1,
            (None, Some(right)) => right.depth() + 1,
            (None, None) => 1,
        }
    }
}

#[cfg(test)]
mod test {
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::aa_tree::Node;

    extern crate quickcheck_macros;

    #[test]
    fn should_create_tree() {
        let node = Node::new_leaf(2);
        let (values, _) = node.collect_to_vec();
        assert_eq!(values, vec![2]);

        let node = node.put(5);
        let (values, _) = node.collect_to_vec();
        assert_eq!(values, vec![2, 5]);

        let node = node.put(1);
        let (values, _) = node.collect_to_vec();
        assert_eq!(values, vec![1, 2, 5]);

        let node = node.put(3);
        let (values, _) = node.collect_to_vec();
        assert_eq!(values, vec![1, 2, 3, 5]);

        let node = node.put(4);
        let (values, _) = node.collect_to_vec();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn should_not_mutate_existing_tree() {
        let node = Node::new_leaf(2);
        let node = node.put(3);
        let node = node.put(2);
        let node = node.put(4);
        let node = node.put(11);
        let node = node.put(5);

        let (values, levels) = node.collect_to_vec();
        assert_eq!(values, vec![2, 3, 4, 5, 11]);
        assert_eq!(levels, vec![1, 2, 1, 2, 1]);

        let node2 = node.put(6);
        let (values, levels) = node2.collect_to_vec();
        assert_eq!(values, vec![2, 3, 4, 5, 6, 11]);
        assert_eq!(levels, vec![1, 2, 1, 2, 1, 1]);

        let (values, levels) = node.collect_to_vec();
        assert_eq!(values, vec![2, 3, 4, 5, 11]);
        assert_eq!(levels, vec![1, 2, 1, 2, 1]);
    }

    #[test]
    // // https://cs.valdosta.edu/~dgibson/courses/cs3410/notes/ch19_6.pdf
    fn should_build_example_1() {
        let node = Node::new_leaf(10);
        let node = node.put(85);
        let node = node.put(15);
        let node = node.put(70);
        let node = node.put(20);
        let node = node.put(60);
        let node = node.put(30);
        let node = node.put(50);
        let node = node.put(65);
        let node = node.put(80);
        let node = node.put(90);
        let node = node.put(40);
        let node = node.put(5);
        let node = node.put(55);
        let node = node.put(35);
        let node = node.put(95);
        let node = node.put(99);

        let (values, levels) = node.collect_to_vec();
        assert_eq!(values, vec![5, 10, 15, 20, 30, 35, 40, 50, 55, 60, 65, 70, 80, 85, 90, 95, 99]);
        assert_eq!(levels, vec![1, 1, 2, 1, 3, 1, 1, 2, 1, 2, 1, 3, 1, 2, 1, 2, 1]);
    }

    #[quickcheck]
    fn should_build_balanced_tree(mut input: Vec<u32>) -> TestResult {
        if input.is_empty() {
            TestResult::discard()
        } else {
            let (first, rest) = input.split_at(1);
            let node = Node::new_leaf(*first.get(0).unwrap());
            let node = rest.iter().fold(node, |acc, v| acc.put(*v));

            let (values, _) = node.collect_to_vec();
            input.sort();
            input.dedup();

            let depth = node.depth();
            let log2_len = input.len().ilog2();

            TestResult::from_bool(values == input && depth <= log2_len * 2 + 1)
        }
    }
}
