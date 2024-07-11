package kafka

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
)

func Publish(conn net.Conn, message Message, topic, path string) error {
	filePath := fmt.Sprintf("%s/%s.topic", path, topic)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	raw = append(raw, byte('\n'))
	_, err = file.Write(raw)

	return err
}
