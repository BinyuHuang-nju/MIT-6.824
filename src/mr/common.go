package mr

type TaskPhase int

const(
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	nMap     int
	nReduce  int
	seq      int
	phase    TaskPhase
	filename string
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
