package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	_ "os"
	"strings"
	"sync"
	_ "time"

	"github.com/bxcodec/faker/v3"
	_ "github.com/lib/pq"
	"github.com/tjarratt/babble"
	"github.com/tkanos/gonfig"
)

var (
	US_STATES = []string{"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"}
	GENDER    = []string{"M", "F"}
	INSURANCE = []string{"ALTADENA", "BLUE SHIELD", "KAISER", "ANTHEM", "BLUE CROSS"}
	ADTS      = []string{"08", "01", "02", "04", "32", "64"}
)

type Config struct {
	ReportDB struct {
		Host   string `json:"host"`
		Port   string `json:"port"`
		DBName string `json:"dbname"`
		DBUser string `json:"dbuser"`
		DBPass string `json:"dbuserpass"`
	} `json:"reportdb"`
}

func readConfig() Config {
	var config Config
	err := gonfig.GetConf("etl.config.json", &config)
	if err != nil {
		log.Fatal("Error reading config file:", err)
	}
	return config
}

func connectDB(config Config) *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		config.ReportDB.Host, config.ReportDB.Port, config.ReportDB.DBName, config.ReportDB.DBUser, config.ReportDB.DBPass)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error connecting to the database:", err)
	}
	return db
}

func insertData(db *sql.DB, table string, wg *sync.WaitGroup) {
	defer wg.Done()

	numFiles := 2000000
	for i := 0; i < numFiles; i++ {
		pid := fmt.Sprintf("%d", rand.Intn(9000000)+1000000)
		// filename := fmt.Sprintf("pid_%s_%s.json", pid, ksuid.New().String())

		//fake := faker.
		// fake.RandomElement(ADTS)
		updatedat := faker.Date()
		dob := faker.Date()
		dobint := strings.ReplaceAll(dob, "-", "")
		name := strings.Split(faker.Name(), " ")
		adt := "D"

		babbler := babble.NewBabbler()
		babbler.Count = 1
		tenantid := babbler.Babble()
		// insName := babbler.Babble()

		visitnum := fmt.Sprintf("%d", rand.Intn(90000000)+10000000)
		somecode := fmt.Sprintf("S%d", rand.Intn(90000)+10000)

		data := map[string]interface{}{
			"patientId":   pid,
			"tenantId":    tenantid,
			"dob":         dob,
			"id":          pid,
			"updatedAt":   updatedat,
			"createdAt":   updatedat,
			"visitNumber": visitnum,
			"MSH":         fmt.Sprintf("MSH|^~&|EPICCARE|WB^WBPC|||20230110144357|%s|ADT^%s^ADT_A01|400815517|P|2.3", somecode, adt),
			"EVN":         fmt.Sprintf("EVN|%s|20230110144357||REGCHECKCOMP_%s|%s^%s^%s^ANAME^^^^^WB^^^^^WBPC||WBPC^1740348929^SOMENAME", adt, adt, somecode, name[len(name)-1], name[0]),
			"PID":         fmt.Sprintf("PID|1||14891584^^^^EPI~62986117^^^^SOMERN||%s^%s||%s|%s|||%s^^%s^%s^%s^USA^P^^SC", name[0], name[len(name)-1], dobint, "M", faker.GetAddress(), faker.GetAddress(), "CA", "94444"),
			"PV1":         fmt.Sprintf("PV1||O|168 ~219~C~PMA^^^^^^^^^||||277^%s^BONNIE|||||||||| ||2688684|||||||||||||||||||||||||202211031408||||||002376853", name[len(name)-1]),
			"IN1":         fmt.Sprintf("IN1|1|PRE2||%s|PO BOX 23523^WELLINGTON^ON^98111|||19601||||||||%s^%s^M|F|||||||||||||||||||ZKA%s", "BLUE", name[len(name)-1], name[0], visitnum),
		}

		// Convert data to JSON
		sample, err := json.Marshal(data)
		if err != nil {
			log.Fatal("Error marshaling JSON:", err)
		}

		// Insert data into PostgreSQL
		query := fmt.Sprintf("INSERT INTO %s (patientjson) values ($1);", table)
		_, err = db.Exec(query, sample)
		if err != nil {
			log.Fatal("Error executing query:", err)
		}
	}
}

func main() {
	config := readConfig()
	db := connectDB(config)

	// PostgreSQL table
	table := "jsondocs"

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go insertData(db, table, &wg)
	}

	wg.Wait()
}
