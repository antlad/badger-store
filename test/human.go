package test

import "github.com/google/uuid"

type Human struct {
	Id     uuid.UUID
	Age    int
	Name   string
	Email  string
	Status string
}
