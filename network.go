package main

import (
	"database/sql"
	"net/url"
	"strconv"
	"sync"

	vkapi "github.com/demonoid81/vk-public-wall-parser/vk-api"
	"github.com/sirupsen/logrus"
)

type AppSettings struct {
	AppID      string `json:"app_id"`
	APIVersion string `json:"api_version"`
	userdata   map[string]string
}

func login(settings *AppSettings) (*vkapi.VKClient, error) {
	client, err := vkapi.NewVKClient(vkapi.DeviceIPhone, settings.userdata["email"], settings.userdata["pass"], true)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func getAllPosts(db *sql.DB, settings AppSettings, client *vkapi.VKClient) error {
	logrus.Info("Start getting info from posts ...")

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	ownerIDInt, _ := strconv.Atoi(settings.userdata["source"])
	countOfPosts, _ := strconv.Atoi(settings.userdata["count_of_posts"])

	logrus.Info("Id public: ", ownerIDInt)

	params := url.Values{}
	params.Set("offset", strconv.Itoa(20))
	var posts *vkapi.Wall

	if countOfPosts <= 100 {
		logrus.Info("Count of need parse posts less than 100: ", countOfPosts)

		posts, _ := client.WallGet(ownerIDInt, countOfPosts, params)
		logrus.Info("All count in this public: ", posts.Count)
		logrus.Info("Saving posts from ", 0, " to ", countOfPosts)

		savePosts(db, posts.Posts)
	} else {
		logrus.Info("Count of need parse posts more than 100: ", countOfPosts)
		posts, _ = client.WallGet(ownerIDInt, 1, params)
		logrus.Info("All count in this public: ", posts.Count)

		if posts.Count < countOfPosts {
			countOfPosts = posts.Count
		}

		for i := 0; i < countOfPosts/100; i++ {
			wg.Add(1)
			paramsTmpf := url.Values{}

			go func(i int, wg *sync.WaitGroup, mu *sync.Mutex) {
				defer wg.Done()
				logrus.Info("Getting posts from ", i*100, " to ", (i+1)*100)

				paramsTmpf.Set("offset", strconv.Itoa(100*i))
				posts100, err := client.WallGet(ownerIDInt, countOfPosts, paramsTmpf)

				if err == nil {
					logrus.Info("Saving posts from ", i*100, " to ", (i+1)*100)
					mu.Lock()
					savePosts(db, posts100.Posts)
					mu.Unlock()
				}
			}(i, wg, mu)
		}
	}
	wg.Wait()
	return nil
}
