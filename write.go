package main

import (
	"bytes"
	"fmt"
)

func (c *Config) push(url *string) error {

	resp, err := client.Post(*url, jsonContentType, bytes.NewBuffer([]byte("")))
	if err != nil {
		return err
	}
	fmt.Print(resp)
	return nil
}
