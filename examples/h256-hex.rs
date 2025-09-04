use primitive_types::H256;

fn main() {
    println!("{:x}", H256::from_low_u64_le(42));
}
