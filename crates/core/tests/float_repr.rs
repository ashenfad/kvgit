//! Python's float `repr`, checked against what CPython wrote for a few
//! thousand doubles (`fixtures/float-repr.txt`).

use kvgit_core::json::{Value, float_repr, loads};

#[test]
fn every_double_writes_as_cpython_wrote_it() {
    let text = include_str!("fixtures/float-repr.txt");
    let mut checked = 0;
    for line in text.lines() {
        let (bits, want) = line.split_once(' ').expect("<bits> <repr>");
        let x = f64::from_bits(u64::from_str_radix(bits, 16).expect("hex bits"));
        assert_eq!(float_repr(x), want, "bits {bits}");
        assert_eq!(loads(want.as_bytes()).unwrap(), Value::Float(x), "{want}");
        checked += 1;
    }
    assert!(checked > 3000);
}
