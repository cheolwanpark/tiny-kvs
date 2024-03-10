#[allow(unused_imports)]
use rand::Rng;

pub fn rand_bytes(len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.sample(rand::distributions::Alphanumeric)).collect()
}

pub fn rand_string(len: usize) -> String {
    let v = rand_bytes(len);
    String::from_utf8(v).unwrap()
}

// include min and exclude max
pub fn rand_usize(min: usize, max: usize) -> usize {
    let mut rng = rand::thread_rng();
    rng.gen_range(min..max)
}