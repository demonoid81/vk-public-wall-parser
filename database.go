package main

import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"strings"

	vkapi "github.com/demonoid81/vk-public-wall-parser/vk-api"
	"github.com/sirupsen/logrus"
)

func createTable(db *sql.DB, initstring string) {
	stmt, err := db.Prepare(initstring)
	if err != nil {
		log.Fatal(err)
	}

	_, err = stmt.Exec()

	if err != nil {
		log.Fatal(err)
	}
}

func initDataBase(filepath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}

	createTable(db,
		`CREATE TABLE IF NOT EXISTS posts (
			id INTEGER,
			from_id INTEGER,
			owner_id INTEGER,
			signer_id INTEGER,
			date INTEGER,
			marked_as_ads INTEGER,
			post_type TEXT,
			text TEXT,
			is_pinned INTEGER,
			comments_count INTEGER,
			likes_count INTEGER,
			reposts_count INTEGER,
			attachments_count INTEGER,
			PRIMARY KEY (id, from_id)
		);`)

	createTable(db,
		`CREATE TABLE IF NOT EXISTS attachments (
			type TEXT,
			id INTEGER,
			owner_id INTEGER,
			post_id INTEGER,
			url TEXT,
			additional_info text,
			additional_info2 text,
			PRIMARY KEY (id, type, post_id)
		);`)

	return db, nil
}

func savePosts(db *sql.DB, items []*vkapi.WallPost) error {
	if len(items) == 0 {
		return errors.New("count of posts is null")
	}
	insertposts := `
		INSERT OR IGNORE INTO posts (
			id,
			from_id,
			owner_id,
			signer_id,
			date,
			marked_as_ads,
			post_type,
			text,
			is_pinned,
			comments_count,
			likes_count,
			reposts_count,
			attachments_count
		) VALUES 
	`
	insertattachmentsTemplate := `
		INSERT OR IGNORE INTO attachments (
			type,
			id,
			post_id,
			url,
			additional_info,
			additional_info2
		) VALUES 
	`

	insertattachments := insertattachmentsTemplate
	postsvalues := []interface{}{}
	attachmentsvalues := []interface{}{}
	count := 0

	tx, _ := db.Begin()

	for _, item := range items {
		insertposts += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"
		postsvalues = append(postsvalues, item.ID, item.FromID, item.OwnerID, item.SignerID,
			item.Date, item.MarkedAsAd, item.PostType, item.Text, item.IsPinned,
			item.Comments.Count, item.Likes.Count, item.Reposts.Count,
			len(item.Attachments))

		if len(item.Attachments) > 0 {
			for i, attachment := range item.Attachments {
				count++
				insertattachments += "(?, ?, ?, ?, ?, ?),"
				if attachment.Type == "photo" {
					attachmentsvalues = append(attachmentsvalues, attachment.Type,
						attachment.Photo.ID, item.ID, "photo"+
							strconv.Itoa(attachment.Photo.OwnerID)+"_"+strconv.Itoa(attachment.Photo.ID),
						attachment.Photo.Text, "")
				} else if attachment.Type == "video" {
					attachmentsvalues = append(attachmentsvalues, attachment.Type,
						attachment.Video.ID, item.ID, "photo"+
							strconv.Itoa(attachment.Video.OwnerID)+"_"+strconv.Itoa(attachment.Video.ID),
						attachment.Video.Title, "")
				} else if attachment.Type == "audio" {
					attachmentsvalues = append(attachmentsvalues, attachment.Type,
						attachment.Audio.ID, item.ID, attachment.Audio.URL,
						attachment.Audio.Artist+"-"+attachment.Audio.Title, "")
				} else if attachment.Type == "doc" {
					attachmentsvalues = append(attachmentsvalues, attachment.Type,
						attachment.Document.ID, item.ID, attachment.Document.URL, attachment.Document.Title, "")
				} else if attachment.Type == "link" {
					attachmentsvalues = append(attachmentsvalues, attachment.Type,
						i, item.ID, attachment.Link.URL, attachment.Link.Title+":"+
							attachment.Link.Target, "")
				} else {
					attachmentsvalues = append(attachmentsvalues, attachment.Type,
						item.ID, item.ID, "", "", "")
				}

				if count >= 500 {
					execInserts(tx, insertattachments, attachmentsvalues)
					count = 0
					insertattachments = insertattachmentsTemplate
					attachmentsvalues = []interface{}{}
				}
			}

		}
	}

	execInserts(tx, insertposts, postsvalues)
	if count > 0 {
		execInserts(tx, insertattachments, attachmentsvalues)
	}
	err := tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func execInserts(tx *sql.Tx, insertString string, values []interface{}) {
	insertString = strings.TrimSuffix(insertString, ",")
	stmt, err := tx.Prepare(insertString)
	checkErr(err)
	_, err = stmt.Exec(values...)
	defer stmt.Close()
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		logrus.Fatal(err)
	}
}
