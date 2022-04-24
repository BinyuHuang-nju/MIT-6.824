package kvraft

type KVStateMachine struct {
	data map[string]string
}

func MakeSM() *KVStateMachine {
	sm := &KVStateMachine{
		data: make(map[string]string),
	}
	return sm
}

func (sm *KVStateMachine) GetSM() map[string]string {
	return sm.data
}

func (sm *KVStateMachine) SetSM(data map[string]string) {
	sm.data = data
}

func (sm *KVStateMachine) Get(key string) (string, Err) {
	if val, ok := sm.data[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (sm *KVStateMachine) Put(key string, value string) Err {
	sm.data[key] = value
	return OK
}
func (sm *KVStateMachine) Append(key string, value string) Err {
	sm.data[key] += value
	return OK
}