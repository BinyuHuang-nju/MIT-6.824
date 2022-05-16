# MIT-6.824
Basic Sources for MIT 6.824 Distributed Systems Class

MIT 6.824 课程的学习资料 代码更新为2021版


## lab

### lab1
-	MapReduce Paper(OSDI'06)  
- [x]	Pass

### lab2  
-	Raft Paper(Extended 14)  
- [x]	2A pass  
	- test: sh ./test_election.sh  
- [x]	2B pass
	- test: sh ./test_replication.sh  
- [x]   2C pass  
	- test: sh ./test_persist.sh  
	- [ ] (TestFigure8Unreliable2C often fails) 
- [x]   2D pass
	- test: sh ./test_snapshot.sh  

### lab3
-	OngaroPhD Paper(Sec.6)  
- [x]	3A pass
	- test: sh ./test_kv3A.sh
	- [ ] (TestSpeed3A fail, operations completed too slowly)  
- [x]   3B  
	- test: sh ./test_kv3B.sh  
	- [ ] (TestSpeed3B fail, operations completed too slowly)  
- 目前看系统性能还存在缺陷，可提升角度:
	- client通过更少尝试连接次数与最新leader连接  
	- 读优化，对Get采用ReadIndex或LeaseRead方式减轻log与广播负担  

### lab4
- [x]	4 shard controller pass
	- test: sh ./test.sh
- [ ]	4 shard kv  

## 课程安排 Schedule

[课程安排](https://pdos.csail.mit.edu/6.824/schedule.html)

## 视频 Videos

[2020年lectures视频地址](https://www.bilibili.com/video/av87684880)

## 讲座 Lectures

- [Lec1: 入门介绍(以MapReduce为例)](https://github.com/chaozh/MIT-6.824/issues/2)
- [Lec2: RPC与线程机制(Go语言实战)](https://github.com/chaozh/MIT-6.824/issues/3)
- [Lec3: GFS](https://github.com/chaozh/MIT-6.824/issues/6)
- [Lec4：主从备份](https://github.com/chaozh/MIT-6.824/issues/7)
- [Lec 5：Raft基本](https://github.com/chaozh/MIT-6.824/issues/9)
- [Lec6：Raft实现](https://github.com/chaozh/MIT-6.824/issues/10)

## 问题 Questions

记录在issues中

- 课前问题：[对分布式系统课程有啥想说的？](https://github.com/chaozh/MIT-6.824/issues/1)
- [Lab0 完成Crawler与KV的Go语言实验](https://github.com/chaozh/MIT-6.824/issues/4)
- Lab1 MapReduce实验
- [Lec3 请描述客户端从GFS读数据的大致流程？](https://github.com/chaozh/MIT-6.824/issues/6)
- [Lec4 论文中VM FT如何处理网络分区问题？](https://github.com/chaozh/MIT-6.824/issues/7)
- [Lec5 Raft什么机制会阻止他们当选？](https://github.com/chaozh/MIT-6.824/issues/9)
- [Lec6 Figure13中第8步能否导致状态机重置，即接收InstallSnapshot RPC消息能否导致状态回退](https://github.com/chaozh/MIT-6.824/issues/10)

## 参考资料 Related

- [MapReduce(2004)](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)
- [GFS(2003)](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/gfs-sosp2003.pdf)
- [Fault-Tolerant Virtual Machines(2010)](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)
- [Raft Extended(2014)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)


