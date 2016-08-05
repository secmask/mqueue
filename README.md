### Introduction
[![Circle CI](https://circleci.com/gh/secmask/mqueue.svg?style=svg&circle-token=fdfcf508f51b281788a87d8ed8288372d3b5f488)](https://circleci.com/gh/secmask/mqueue)

**mqueue** implement redis like _list_ but optimize for very long queue, it start with 
 a small part in memory (if we can pop item out fast enough, we will never need to touch to disk), 
 if this queue full, mqueue will switch to memory map file queue mode
 which will only limit by your disk space.

## Installation
1. `> go get github.com/secmask/mqueue`
2. `> go build github.com/secmask/mqueue`
3. `> mqueue`

benchmark it
```
secmask@miner:~$ redis-benchmark -p 1607 -c 32 -n 1000000 lpush k2 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
====== lpush k2 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ======
  1000000 requests completed in 11.51 seconds
  32 parallel clients
  3 bytes payload
  keep alive: 1

99.46% <= 1 milliseconds
99.75% <= 2 milliseconds
99.82% <= 3 milliseconds
99.90% <= 4 milliseconds
99.95% <= 5 milliseconds
99.97% <= 6 milliseconds
99.98% <= 7 milliseconds
99.99% <= 8 milliseconds
100.00% <= 9 milliseconds
100.00% <= 10 milliseconds
100.00% <= 11 milliseconds
100.00% <= 12 milliseconds
100.00% <= 14 milliseconds
100.00% <= 16 milliseconds
86880.97 requests per secon
```
at the same time, on other terminal
```
secmask@miner:~$ redis-benchmark -p 1607 -c 32 -n 1000000 brpop k2 10
====== brpop k2 10 ======
  1000000 requests completed in 11.73 seconds
  32 parallel clients
  3 bytes payload
  keep alive: 1

99.42% <= 1 milliseconds
99.71% <= 2 milliseconds
99.82% <= 3 milliseconds
99.91% <= 4 milliseconds
99.96% <= 5 milliseconds
99.97% <= 6 milliseconds
99.98% <= 7 milliseconds
99.99% <= 8 milliseconds
99.99% <= 9 milliseconds
99.99% <= 10 milliseconds
99.99% <= 11 milliseconds
99.99% <= 12 milliseconds
99.99% <= 13 milliseconds
100.00% <= 14 milliseconds
100.00% <= 16 milliseconds
85236.95 requests per second
```
### License
mqueue is provide under MIT License