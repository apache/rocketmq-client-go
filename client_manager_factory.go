package rocketmq

var clientManagers map[string]IClientManager

func init() {
	clientManagers = make(map[string]IClientManager)
}

func GetClientManager(resourceNamespace string) IClientManager {
	cm, ok := clientManagers[resourceNamespace]
	if ok {
		return cm
	}
	cm = NewClientManage()
	// TODO: configure client managers.
	clientManagers[resourceNamespace] = cm
	// ensure the resourceNamespace of clientMange is unique
	return GetClientManager(resourceNamespace)
}
