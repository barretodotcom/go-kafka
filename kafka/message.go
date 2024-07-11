package kafka

type Message struct {
	Head string      `json:"head"`
	Body interface{} `json:"body"`
}
