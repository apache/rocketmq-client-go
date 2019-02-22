package common

type SendMessageResponse struct {
	MsgId         string
	QueueId       int32
	QueueOffset   int64
	TransactionId string
	MsgRegion     string
}

type PullMessageResponse struct {
	SuggestWhichBrokerId int64
	NextBeginOffset      int64
	MinOffset            int64
	MaxOffset            int64
}
