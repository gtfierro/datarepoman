package main

import (
	"encoding/csv"
	"fmt"
	giles "github.com/gtfierro/giles2/archiver"
	"os"
	"time"
)

type row struct {
	UUID  string
	Time  time.Time
	Value float64
}

func rowsFromSmapMessage(msg *giles.SmapMessage) chan row {
	c := make(chan row)
	go func() {
		for _, rdg := range msg.Readings {
			c <- row{
				UUID:  string(msg.UUID),
				Time:  time.Unix(int64(rdg.GetTime())/1000, 0),
				Value: rdg.GetValue().(float64),
			}
		}
		close(c)
	}()
	return c
}

type destination interface {
	WriteRow(r row) error
}

type StdoutDestination struct {
}

func (stdout *StdoutDestination) WriteRow(r row) error {
	fmt.Printf("%s, %v, %0.2f\n", r.UUID, r.Time, r.Value)
	return nil
}

type CSVDestination struct {
	filename string
	writer   *csv.Writer
}

func CreateCSVDestination(filename string) *CSVDestination {
	dest := &CSVDestination{filename: filename}
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err, "Could not open CSV destination")
	}
	dest.writer = csv.NewWriter(f)
	dest.writer.Write([]string{"UUID", "Time", "Value"})
	dest.writer.Flush()
	return dest
}

func (csvfile *CSVDestination) WriteRow(r row) error {
	if err := csvfile.writer.Write([]string{r.UUID, r.Time.Format(time.RFC3339), fmt.Sprintf("%0.2f", r.Value)}); err != nil {
		return err
	}
	csvfile.writer.Flush()
	return nil
}
