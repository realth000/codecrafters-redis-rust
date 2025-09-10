pub fn num_to_bytes(v: i64) -> Vec<u8> {
    v.to_string()
        .trim_matches(['-', '+'])
        .chars()
        .map(|x| x as u8)
        .collect::<Vec<_>>()
}

pub(crate) fn bytes_to_num(v: impl AsRef<[u8]>) -> i64 {
    v.as_ref()
        .into_iter()
        .rev()
        .enumerate()
        .map(|(idx, x)| {
            if &b'0' <= x && x <= &b'9' {
                ((x - 48) as i64) * 10_i64.pow(idx as u32)
            } else {
                0
            }
        })
        .fold(0, |acc, x| acc + x)
}
