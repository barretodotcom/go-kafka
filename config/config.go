package config

import (
	"os"
	"strconv"
)

type Config struct {
	Host    string
	Port    string
	Address string
	Network string
	Workers int
	Path    string
}

var config *Config

func Load() *Config {
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	network := os.Getenv("NETWORK")
	address := os.Getenv("ADDRESS")
	path := os.Getenv("K_PATH")

	workersStr := os.Getenv("WORKERS")

	workers, err := strconv.Atoi(workersStr)
	if err != nil {
		workers = 1
	}

	config = &Config{
		Host:    host,
		Port:    port,
		Address: address,
		Network: network,
		Workers: workers,
		Path:    path,
	}

	return config
}

func Get() *Config {
	return config
}
