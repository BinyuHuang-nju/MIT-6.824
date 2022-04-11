package mr

type TaskPhase int

const(
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	NMap     int
	NReduce  int
	Seq      int
	Phase    TaskPhase
	Filename string
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
