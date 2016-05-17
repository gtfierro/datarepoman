package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/jinzhu/now"
	"github.com/pkg/errors"
	"strings"
	"time"
)

type downloadParams struct {
	uuids         []string
	uuidChunkSize int
	all           bool
	start         time.Time
	end           time.Time
	legacy        bool
}

func (dl *downloadParams) print() {
	log.Info("--Generated Download Parameters--")
	log.Info(fmt.Sprintf("Start: %s", dl.start.Format("1/2/2006 15:04")))
	log.Info(fmt.Sprintf("End: %s", dl.end.Format("1/2/2006 15:04")))
	if dl.all {
		log.Info(fmt.Sprintf("Downloading all streams. Found %d", len(dl.uuids)))
	} else {
		log.Info(fmt.Sprintf("Downloading %d UUIDs", len(dl.uuids)))
	}

}

func getDownloadParams(c *cli.Context) *downloadParams {
	var (
		start, end time.Time
		err        error
	)
	dl := &downloadParams{
		all:           c.Bool("all"),
		uuids:         strings.Split(c.String("uuids"), ","),
		uuidChunkSize: c.Int("uuidchunk"),
		legacy:        c.Bool("legacy"),
	}
	start, err = now.Parse(c.String("from"))
	if err != nil {
		log.Error(errors.Wrap(err, "Could not parse start value"))
		start = now.BeginningOfDay()
	}
	end, err = now.Parse(c.String("to"))
	if err != nil {
		log.Error(errors.Wrap(err, "Could not parse end value"))
		end = now.EndOfDay()
	}

	dl.start = start
	dl.end = end

	return dl
}

func (dl *downloadParams) ToSmap() string {
	start := dl.start.Unix()
	end := dl.end.Unix()
	if dl.legacy { // turn into milliseconds
		start *= 1000
		end *= 1000
	}
	query := fmt.Sprintf("select data in (%d, %d) where ", start, end)
	for i, uuid := range dl.uuids {
		query = query + fmt.Sprintf("uuid = '%s'", uuid)
		if i < (len(dl.uuids) - 1) {
			query = query + " or "
		} else {
			query = query + ";"
		}
	}
	return query
}

type source interface {
	Connect() error
	AddDestination(dest destination)
	Download(c *cli.Context) error
	GetMetadata(c *cli.Context) error
	LoadMetadata(c *cli.Context) error
	LoadData(c *cli.Context) error
}
