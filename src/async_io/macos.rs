include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub fn test() {
    let kq = unsafe { kqueue() };
    if kq == -1 {
        panic!("Failed to create kqueue");
    }
    println!("kqueue created: {}", kq);
}