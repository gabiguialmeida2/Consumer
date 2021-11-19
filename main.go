package main

import (
	"encoding/json"
	"log"

	"github.com/gabiguialmeida2/rabbitConnection"
	"github.com/streadway/amqp"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var conn *amqp.Connection
var ch *amqp.Channel
var q amqp.Queue
var db *gorm.DB

func CreateConnectionDB() (db *gorm.DB) {
	var err error

	dsn := "host=localhost port=5432 user=postgres dbname=onboarding sslmode=disable password=postgres"
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		log.Fatalf("Ocorreu um erro ao conectar com a base %v", err)
	}

	return
}

func main() {

	conn, ch = rabbitConnection.CreateConnection()
	db = CreateConnectionDB()
	defer conn.Close()
	defer ch.Close()

	q = rabbitConnection.DeclareQueue(conn, ch)

	msgs := rabbitConnection.ConsumeMessages(ch, q)

	err := db.AutoMigrate(&rabbitConnection.Pessoa{})
	if err != nil {
		log.Fatalf("Ocorreu um erro ao conectar com a base %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			pessoa := rabbitConnection.Pessoa{}
			json.Unmarshal(d.Body, &pessoa)

			pessoaExistente := rabbitConnection.Pessoa{}
			db.Where("cpf = ?", pessoa.Cpf).First(&pessoaExistente)

			if pessoaExistente.ID != 0 {
				log.Printf("Não é possível inserir pessoa com o CPF %v", pessoaExistente.Cpf)
				continue
			}
			db.Create(&pessoa)
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	<-forever
}
