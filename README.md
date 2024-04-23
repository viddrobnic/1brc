# The One Billion Row Challenge

At the beginning of the year, [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc) gained popularity. The challenge is to process a billion rows of data as fast as possible. The original challenge focuses on solutions in Java and does not accept solutions in other languages. This repository contains my attempt at the challenge, but in Rust instead of Java. Once I achieved sub 10s, I decided it was good enough and moved on to the next thing 😄

## Times

I measured the progress I made on my machine, which is an M1 MacBook Pro. The following table shows the measurements.

| Name                                    | Time   |
| --------------------------------------- | ------ |
| Java Baseline                           | 3m 20s |
| Rust unoptimized                        | 1m 35s |
| Rust multicore                          | 34s    |
| Rust with improved read allocations     | 24s    |
| Rust with optimized reading and parsing | 6s     |
