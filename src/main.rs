use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead},
};

use memmap2::MmapOptions;

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

    fn update(&self, temp: f32) -> Self {
        Stat {
            min: self.min.min(temp),
            max: self.max.max(temp),
            sum: self.sum + temp,
            count: self.count + 1,
        }
    }
}

fn main() {
    let file = File::open("/Users/vidd/Desktop/1brc/measurements.txt").unwrap();
    let lines = io::BufReader::new(file).lines().flatten();

    let mut stats: HashMap<String, Stat> = HashMap::new();

    for line in lines {
        let (name, temp) = parse_line(line);

        match stats.get(&name) {
            None => {
                stats.insert(name, Stat::new(temp));
            }
            Some(stat) => {
                stats.insert(name, stat.update(temp));
            }
        }
    }

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

fn parse_line(mut line: String) -> (String, f32) {
    let idx = line.find(';').unwrap();
    let temp_str = &line[(idx + 1)..];
    let temp: f32 = temp_str.parse().unwrap();

    line.truncate(idx);

    (line, temp)
}
