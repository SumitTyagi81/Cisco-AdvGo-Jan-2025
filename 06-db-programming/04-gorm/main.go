package main

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Student struct {
	ID          int
	Fname       string
	Lname       string
	DateOfBirth time.Time
	Email       string
	Address     string
	Gender      string
}

func main() {
	// refer https://github.com/go-sql-driver/mysql#dsn-data-source-name for details
	db, err := gorm.Open(mysql.Open("root:rootuser@tcp(localhost:3306)/go_db_demo?parseTime=true"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected!")

	//initializing record to be inserted
	s := Student{
		Fname:       "Jeevan",
		Lname:       "Kuppan",
		DateOfBirth: time.Date(1994, time.August, 14, 23, 51, 42, 0, time.UTC),
		Email:       "jeevan@jeevs.com",
		Address:     "39 Kipling Pass",
		Gender:      "Male",
	}
	//adds student record and returns the ID into the ID field
	db.Create(&s)
	fmt.Printf("addSudent id: %v \n", s.ID)

	//selecting multiple record
	var students []Student
	db.Limit(10).Find(&students)
	fmt.Printf("fetchStudents count: %v \n", len(students))
	for _, stud := range students {
		fmt.Printf("%+v\n", stud)
	}
}
