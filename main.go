package main

import (
	"fmt"
	"log"
	"os"

	"github.com/sirupsen/logrus"
)

func main() {
	settings, err := loadSettings("settings.json")
	if err != nil {
		log.Fatal("Settings load is failed. Closing...")
	}

	userdata, err := loadJSONFileMap("userdata.json")
	if err != nil {
		log.Fatal("Userdata load failed. Closing...")
	}

	settings.userdata = userdata
	client, err := login(&settings)
	if err != nil {
		log.Fatal(err)
	}

	dbPath := fmt.Sprintf("./data_%s.db", settings.userdata["source"])

	if _, err := os.Stat(dbPath); os.IsExist(err) {
		err = os.Remove(dbPath)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	logrus.Info("Name of database: ", dbPath)

	db, err := initDataBase(dbPath)

	if err != nil {
		log.Fatal(err)
	}

	_ = getAllPosts(db, settings, client)
}
