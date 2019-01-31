package main

import "io/ioutil"

type input struct {
	ID string `json:"_id"`
}

func (c *Config) read(url *string) ([]byte, error) {
	resp, err := client.Get(*url)
	if err != nil {
		panic(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return body, nil
}
