use concurrent_hash_map::Map;
use crossbeam::epoch::pin;
use rand::prelude::*;
use std::hash::Hash;

const PRESENT_KEYS_SIZE: usize = 50_000;
const ABSENT_KEYS_SIZE: usize = 1 << 17;

fn insert_keys<K>(map: &Map<K, usize>, keys: &[K], expect: usize)
where
    K: Hash + Eq + Copy + Sync + Send,
{
    let mut sum = 0;
    for key in keys {
        if map.insert(*key, 0).is_none() {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn get_keys<K, V>(map: &Map<K, V>, keys: &[K], expect: usize)
where
    K: Hash + Eq + Clone + Sync + Send,
    V: Sync + Send,
{
    let guard = &pin();
    let mut sum = 0;
    let iters = 4;
    for _ in 0..iters {
        for key in keys {
            if map.get(key, guard).is_some() {
                sum += 1;
            }
        }
    }
    assert_eq!(sum, iters * expect);
}

#[test]
fn insert_and_get() {
    let map = Map::new();
    let mut keys: Vec<_> = (0..PRESENT_KEYS_SIZE + ABSENT_KEYS_SIZE).collect();
    let mut rng = rand::rng();
    keys.shuffle(&mut rng);
    let present_keys = &keys[0..PRESENT_KEYS_SIZE];
    let absent_keys = &keys[PRESENT_KEYS_SIZE..];
    // put (absent)
    insert_keys(&map, present_keys, PRESENT_KEYS_SIZE);
    // put (present)
    insert_keys(&map, present_keys, 0);
    // get (present)
    get_keys(&map, present_keys, PRESENT_KEYS_SIZE);
    // get (absent)
    get_keys(&map, absent_keys, 0);
}
