package primitive

// Message model defines the way how messages are delivered to each consumer clients.
// </p>
//
// RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
// the same {@link #ConsumerGroup} would only consume shards of the messages subscribed, which achieves load
// balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
// separately.
// </p>
//
// This field defaults to clustering.
type MessageModel int

const (
	BroadCasting MessageModel = iota
	Clustering
)

func (mode MessageModel) String() string {
	switch mode {
	case BroadCasting:
		return "BroadCasting"
	case Clustering:
		return "Clustering"
	default:
		return "Unknown"
	}
}

// Consuming point on consumer booting.
// </p>
//
// There are three consuming points:
// <ul>
// <li>
// <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
// If it were a newly booting up consumer client, according aging of the consumer group, there are two
// cases:
// <ol>
// <li>
// if the consumer group is created so recently that the earliest message being subscribed has yet
// expired, which means the consumer group represents a lately launched business, consuming will
// start from the very beginning;
// </li>
// <li>
// if the earliest message being subscribed has expired, consuming will start from the latest
// messages, meaning messages born prior to the booting timestamp would be ignored.
// </li>
// </ol>
// </li>
// <li>
// <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
// </li>
// <li>
// <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
// messages born prior to {@link #consumeTimestamp} will be ignored
// </li>
// </ul>
type ConsumeFromWhere int

const (
	ConsumeFromLastOffset ConsumeFromWhere = iota
	ConsumeFromFirstOffset
	ConsumeFromTimestamp
)

type ExpressionType string

const (
	/**
	 * <ul>
	 * Keywords:
	 * <li>{@code AND, OR, NOT, BETWEEN, IN, TRUE, FALSE, IS, NULL}</li>
	 * </ul>
	 * <p/>
	 * <ul>
	 * Data type:
	 * <li>Boolean, like: TRUE, FALSE</li>
	 * <li>String, like: 'abc'</li>
	 * <li>Decimal, like: 123</li>
	 * <li>Float number, like: 3.1415</li>
	 * </ul>
	 * <p/>
	 * <ul>
	 * Grammar:
	 * <li>{@code AND, OR}</li>
	 * <li>{@code >, >=, <, <=, =}</li>
	 * <li>{@code BETWEEN A AND B}, equals to {@code >=A AND <=B}</li>
	 * <li>{@code NOT BETWEEN A AND B}, equals to {@code >B OR <A}</li>
	 * <li>{@code IN ('a', 'b')}, equals to {@code ='a' OR ='b'}, this operation only support String type.</li>
	 * <li>{@code IS NULL}, {@code IS NOT NULL}, check parameter whether is null, or not.</li>
	 * <li>{@code =TRUE}, {@code =FALSE}, check parameter whether is true, or false.</li>
	 * </ul>
	 * <p/>
	 * <p>
	 * Example:
	 * (a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)
	 * </p>
	 */
	SQL92 = ExpressionType("SQL92")

	/**
	 * Only support or operation such as
	 * "tag1 || tag2 || tag3", <br>
	 * If null or * expression, meaning subscribe all.
	 */
	TAG = ExpressionType("TAG")
)

func IsTagType(exp string) bool {
	if exp == "" || exp == "TAG" {
		return true
	}
	return false
}

type MessageSelector struct {
	Type       ExpressionType
	Expression string
}

type ConsumeResult int

const (
	ConsumeSuccess ConsumeResult = iota
	ConsumeRetryLater
)

type ConsumeMessageContext struct {
	ConsumerGroup string
	Msgs          []*MessageExt
	MQ            *MessageQueue
	Success       bool
	Status        string
	// mqTractContext
	Properties map[string]string
}

type ConsumeResultHolder struct {
	ConsumeResult
}
