use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, Read, Seek, SeekFrom},
};

const PATH: &str = "/Users/vidd/Desktop/1brc/measurements.txt";

struct Stat {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Stat {
    fn new(temp: f32) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    fn update(&mut self, temp: f32) {
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

    let stats: HashMap<String, Stat> =
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
        let mean = stat.sum / stat.count as f32;
        print!("{}={:.1}/{:.1}/{:.1}", key, stat.min, mean, stat.max);
    }
    println!("}}");
}

fn read_part(start: u64, len: usize) -> HashMap<String, Stat> {
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

    let mut stats: HashMap<String, Stat> = HashMap::new();

    let mut reader = io::BufReader::new(&file);
    let mut line = String::new();

    loop {
        if read > len {
            break;
        }

        line.clear();
        let cur_read = reader.read_line(&mut line).unwrap();
        if cur_read == 0 {
            break;
        }
        read += cur_read;

        let (name, temp) = parse_line(&line);
        match stats.get_mut(name) {
            None => {
                stats.insert(name.to_string(), Stat::new(temp));
            }
            Some(stat) => {
                stat.update(temp);
            }
        }
    }

    stats
}

fn parse_line(line: &str) -> (&str, f32) {
    let mut parts = line[..line.len() - 1].split(';');
    let name = parts.next().unwrap();
    let temp_str = parts.next().unwrap();
    let temp: f32 = temp_str.parse().unwrap();

    (name, temp)
}
