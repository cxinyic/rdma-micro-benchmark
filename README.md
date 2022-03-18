# rdma-micro-benchmark
There are three versions for this micro-benchmark.
# v1: two two-sided rdma
compute node:
```
two_sided_rdma_write(PAGE)
two_sided_rdma_read(PAGE)  
process(local_read(PAGE))
```
memory node:
```
while:
  accept RPCs
```
# v2: two one-sided rdma from memory
compute node:
```
local_write(addr + ready_flag)
process(local_read(PAGE))
```
memory node:
```
one_sided_rdma_read(addr + ready_flag)
while (not ready):
one_sided_rdma_read(PAGE)
one_sided_rdma_write(PAGE)
one_sided_rdma_write(ready_flag) //reset ready flag
```
# v3: two one-sided rdma from memory
compute node:
```
one_sided_rdma_write(PAGE) 
one_sided_rdma_read(PAGE)
process(local_read(PAGE))
```
memory node:
 ```
// do nothing
```
