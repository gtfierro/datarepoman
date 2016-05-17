package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	MIME_TEXT = "text/plain"
	MIME_JSON = "application/json"
)

type sMAPSourceParams struct {
	host    string
	uuids   []string
	all     bool
	timeout time.Duration
}

// Connects to a sMAP instance
type sMAPSource struct {
	host         string
	destinations []destination
	client       *http.Client
	timeout      time.Duration
}

func NewSmapSource(params sMAPSourceParams) *sMAPSource {
	smap := &sMAPSource{
		host:    params.host,
		timeout: params.timeout,
	}
	return smap
}

func SmapSourceFromParams(c *cli.Context) *sMAPSource {
	return &sMAPSource{
		host:    c.GlobalString("address"),
		timeout: c.Duration("timeout"),
	}
}

func (smap *sMAPSource) Connect() error {
	smap.client = &http.Client{Timeout: smap.timeout}
	resp, err := smap.client.Head(smap.host)
	if err == nil {
		log.Info(fmt.Sprintf("HEAD gave response: %s", resp.Status))
	}
	return err
}

func (smap *sMAPSource) AddDestination(dest destination) {
	smap.destinations = append(smap.destinations, dest)
}

func (smap *sMAPSource) Download(c *cli.Context) error {
	var resp *http.Response
	var err error
	// if getting all UUIDs, need to fetch from server
	params := getDownloadParams(c)
	var buf *bytes.Buffer
	if params.all {
		buf = bytes.NewBufferString("select distinct uuid where has uuid;")
	} else if c.String("where") != "" {
		buf = bytes.NewBufferString(fmt.Sprintf("select distinct uuid where %s;", c.String("where")))
	} else {
		return smap.startDownloadLoop(params)
	}
	resp, err = smap.client.Post(smap.host, MIME_TEXT, buf)
	if err != nil {
		return errors.Wrap(err, "Could not post query to sMAP archiver")
	}
	var decoder = json.NewDecoder(resp.Body)
	if err := decoder.Decode(&params.uuids); err != nil {
		return errors.Wrap(err, "Could not decode JSON from sMAP archiver")
	}
	return smap.startDownloadLoop(params)
}

func (smap *sMAPSource) startDownloadLoop(params *downloadParams) error {
	var err error
	params.print()
	for downloadChunk := range getUUIDChunks(params) {
		log.Debug("Generated sMAP query:", downloadChunk.ToSmap())
		if err = smap.doDownload(downloadChunk); err != nil {
			return err
		}
	}
	return nil
}

func (smap *sMAPSource) doDownload(params downloadParams) error {
	var buf = bytes.NewBufferString(params.ToSmap())
	resp, err := smap.client.Post(smap.host, MIME_TEXT, buf)
	if err != nil {
		return errors.Wrap(err, "Could not post query to sMAP archiver")
	}
	var data []smapMessage
	var databuf bytes.Buffer
	tee := io.TeeReader(resp.Body, &databuf)
	var decoder = json.NewDecoder(tee)
	if err := decoder.Decode(&data); err != nil {
		message, _ := ioutil.ReadAll(&databuf)
		log.Errorf("Got message from archiver: %v", string(message))
		return errors.Wrap(err, "Could not decode JSON from sMAP archiver")
	}
	for _, msg := range data {
		for _, dest := range smap.destinations {
			for row := range rowsFromSmapMessage(&msg) {
				dest.WriteRow(row)
			}
		}
	}
	return nil
}

type smapMessage struct {
	// Readings for this message
	Readings []reading
	// Unique identifier for this stream. Should be empty for Collections
	UUID string `json:"uuid"`
}

// Reading implementation for numerical data
type reading struct {
	// uint64 timestamp
	Time uint64
	// value associated with this timestamp
	Value float64
}

func (rdg *reading) UnmarshalJSON(b []byte) (err error) {
	var (
		v          []interface{}
		time       uint64
		time_weird float64
		value      float64
		ok         bool
	)
	if err = json.Unmarshal(b, &v); err != nil {
		return errors.Wrap(err, "Could not decode JSON readings")
	}

	if len(v) != 2 {
		return errors.Wrap(err, "Bad sMAP reading. Need 2-tuples of (time,value)")
	}

	if time, ok = v[0].(uint64); !ok {
		if time_weird, ok = v[0].(float64); !ok {
			err = errors.Wrap(err, "Bad timestamp")
			return
		}
		time = uint64(time_weird)
	}

	if value, ok = v[1].(float64); !ok {
		err = errors.Wrap(err, "Bad value")
		return
	}

	rdg.Time = time
	rdg.Value = value
	return
}
