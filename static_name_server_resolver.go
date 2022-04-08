package rocketmq

type StaticStaticNameServerResolver struct {
	nameServerList []string
}

func NewStaticStaticNameServerResolver(nameServerList []string) *StaticStaticNameServerResolver {
	n := &StaticStaticNameServerResolver{
		nameServerList: nameServerList,
	}
	return n
}

func (n *StaticStaticNameServerResolver) ResolveAsync() []string {
	if n == nil {
		return nil
	}
	return n.nameServerList
}
