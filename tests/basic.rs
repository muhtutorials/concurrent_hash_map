use std::sync::Arc;
use std::thread::spawn;
use crossbeam::epoch::pin;
use concurrent_hash_map::*;

#[test]
fn new() {
    let _map = Map::<&str, u8>::new();
}

#[test]
fn insert() {
    let map = Map::<&str, u8>::new();
    let old = map.insert("Jon", 42);
    assert!(old.is_none())
}

#[test]
fn get_empty() {
    let map = Map::<&str, u8>::new();
    let guard = &pin();
    let value = map.get(&"Jon", guard);
    assert!(value.is_none())
}

#[test]
fn insert_and_get() {
    let map = Map::<&str, u8>::new();
    map.insert("Jon", 42);
    let guard = &pin();
    let value = map.get(&"Jon", guard).unwrap();
    let value = unsafe { value.deref() };
    assert_eq!(*value, 42)
}

#[test]
fn concurrent_insert() {
    let map = Arc::new(Map::<u8, u8>::new());
    let map_1 = map.clone();
    let t1 = spawn(move || {
       for i in 0..128 {
           map_1.insert(i, 0);
       }
    });
    let map_2 = map.clone();
    let t2 = spawn(move || {
        for i in 0..128 {
            map_2.insert(i, 1);
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();
    let guard = &pin();
    for i in 0..128 {
        let value = map.get(&i, guard).unwrap();
        let value = unsafe { value.deref() };
        assert!(*value == 0 || *value == 1);
    }
}

#[test]
fn update() {
    let map = Map::<&str, u8>::new();
    map.insert("Jon", 42);
    let old = map.insert("Jon", 43);
    assert!(old.is_some());
    let guard = &pin();
    let value = map.get(&"Jon", guard).unwrap();
    let value = unsafe { value.deref() };
    assert_eq!(*value, 43)
}

#[test]
fn current_kv_dropped() {
    let dropped_1 = Arc::new(0);
    let dropped_2 = Arc::new(1);
    let map = Map::<Arc<usize>, Arc<usize>>::new();
    map.insert(dropped_1.clone(), dropped_2.clone());
    assert_eq!(Arc::strong_count(&dropped_1), 2);
    assert_eq!(Arc::strong_count(&dropped_2), 2);
    drop(map);
    assert_eq!(Arc::strong_count(&dropped_1), 1);
    assert_eq!(Arc::strong_count(&dropped_2), 1);
}

#[test]
#[ignore]
// ignored because we have no control over when destructors run
fn drop_value() {
    let dropped_1 = Arc::new(0);
    let dropped_2 = Arc::new(1);
    let map = Map::<usize, Arc<usize>>::new();
    map.insert(1, dropped_1.clone());
    assert_eq!(Arc::strong_count(&dropped_1), 2);
    assert_eq!(Arc::strong_count(&dropped_2), 1);
    map.insert(1, dropped_2.clone());
    drop(map);
    assert_eq!(Arc::strong_count(&dropped_1), 1);
    assert_eq!(Arc::strong_count(&dropped_2), 1);
}