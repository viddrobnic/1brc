use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

const PATH: &str = "/Users/vidd/Desktop/1brc/measurements.txt";

struct Stat {
    min: i64,
    max: i64,
    sum: i64,
    count: u32,
}

impl Stat {
    fn new(temp: i64) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    fn update(&mut self, temp: i64) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.count += 1;
    }

    fn join(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

struct ParsedLine<'a> {
    data_read: usize,
    name: &'a [u8],
    temp: i64,
}

fn main() {
    let file = File::open(PATH).unwrap();
    let len = file.metadata().unwrap().len();

    let cores = std::thread::available_parallelism().unwrap().get() as u64;
    let per_core = len / cores;

    let threads: Vec<_> = (0..cores)
        .map(|c| {
            std::thread::Builder::new()
                .name(format!("Thread {}", c))
                .spawn(move || read_part(per_core * c, per_core as usize))
                .unwrap()
        })
        .collect();

    let stats: HashMap<Vec<u8>, Stat> =
        threads.into_iter().fold(HashMap::new(), |mut acc, handle| {
            let res = handle.join().unwrap();
            for (key, stat) in res.into_iter() {
                match acc.get_mut(&key) {
                    None => {
                        acc.insert(key, stat);
                    }
                    Some(curr_stat) => curr_stat.join(&stat),
                }
            }

            acc
        });

    let mut keys: Vec<_> = stats.keys().collect();
    keys.sort();

    print!("{{");
    for (i, key) in keys.into_iter().enumerate() {
        if i > 0 {
            print!(", ")
        }

        let stat = stats.get(key).unwrap();
        let mean = stat.sum as f32 / stat.count as f32;
        print!(
            "{}={:.1}/{:.1}/{:.1}",
            std::str::from_utf8(&key).unwrap(),
            stat.min as f32 / 10.0,
            mean / 10.0,
            stat.max as f32 / 10.0
        );
    }
    println!("}}");
}

fn read_part(start: u64, len: usize) -> HashMap<Vec<u8>, Stat> {
    let mut file = File::open(PATH).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();

    let mut read: usize = 0;
    if start != 0 {
        let mut buf: [u8; 256] = [0; 256];
        file.read(&mut buf).unwrap();
        let add = buf.iter().position(|c| *c == b'\n').unwrap() as u64;
        file.seek(SeekFrom::Start(start + add + 1)).unwrap();

        read += (add + 1) as usize;
    }

    let mut stats: HashMap<Vec<u8>, Stat> = HashMap::new();

    let mut buf = [0 as u8; 1024 * 1024]; // 100 MB
    let mut start = 0;
    let mut end = 0;

    loop {
        let cur_read = file.read(&mut buf[end..]).unwrap();
        if cur_read == 0 {
            return stats;
        }
        end += cur_read;

        while let Some(line) = parse_line(&buf[start..end]) {
            if read > len {
                return stats;
            }
            read += line.data_read;
            start += line.data_read;

            match stats.get_mut(line.name) {
                None => {
                    stats.insert(line.name.to_owned(), Stat::new(line.temp));
                }
                Some(stat) => {
                    stat.update(line.temp);
                }
            }
        }

        buf.rotate_left(start);
        end -= start;
        start = 0;
    }
}

fn parse_line<'a>(data: &'a [u8]) -> Option<ParsedLine<'a>> {
    let idx = data.iter().position(|c| *c == b';');
    let Some(name_end) = idx else {
        return None;
    };

    let idx = (data[name_end..]).iter().position(|c| *c == b'\n');
    let Some(line_end) = idx else {
        return None;
    };
    let line_end = name_end + line_end;

    let res = ParsedLine {
        data_read: line_end + 1,
        name: &data[..name_end],
        temp: parse_temp(&data[(name_end + 1)..line_end]),
    };
    Some(res)
}

fn parse_temp(temp: &[u8]) -> i64 {
    let mut mult = 1;
    let mut n = 0;

    for ch in temp.iter() {
        match ch {
            b'-' => mult = -1,
            b'.' => (),
            b'0'..=b'9' => n = n * 10 + (ch - b'0') as i64,
            _ => panic!("invalid char: {}", ch),
        }
    }

    n * mult
}
