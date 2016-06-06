package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
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
		if len(msg.Readings) == 100000 { // need to fetch again
			// get last date
			newparams := params
			newparams.start = time.Unix(int64(msg.Readings[len(msg.Readings)-1].Time)/1000, 0)
			newparams.uuids = []string{msg.UUID}
			log.Infof("Start new loop for %s", msg.UUID)
			params.print()
			smap.doDownload(newparams)
		}
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

func (smap *sMAPSource) GetMetadata(c *cli.Context) error {
	var resp *http.Response
	var err error
	var query = bytes.NewBufferString(fmt.Sprintf("select * where %s;", c.String("where")))
	resp, err = smap.client.Post(smap.host, MIME_TEXT, query)
	if err != nil {
		return errors.Wrap(err, "Could not post query to sMAP archiver")
	}
	var metadata []map[string]interface{}
	var decoder = json.NewDecoder(resp.Body)
	if err := decoder.Decode(&metadata); err != nil {
		return errors.Wrap(err, "Could not download metadata from sMAP archiver")
	}

	f, err := os.Create("metadata.json")
	if err != nil {
		log.Fatal(err, "Could not open JSON destination")
	}
	writer := json.NewEncoder(f)
	return writer.Encode(metadata)
}

func (smap *sMAPSource) LoadMetadata(c *cli.Context) error {
	var metadata []smapMessage
	var resp *http.Response
	var err error
	file, err := os.Open(c.String("metadata"))
	if err != nil {
		return errors.Wrap(err, "Could not open metadata file")
	}
	var decoder = json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return errors.Wrap(err, "Could no decode metadata file")
	}
	for _, doc := range metadata {
		var ingestmd = make(map[string]smapMessage)
		if doc.Path == "" {
			log.Error("Doc has no Path")
			continue
		}
		if doc.UUID == "" {
			log.Error("Doc has no UUID")
			continue
		}
		path := doc.Path
		//doc.Path = ""
		ingestmd[path] = doc
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		if err := enc.Encode(ingestmd); err != nil {
			return errors.Wrap(err, "Could not ingest metadata")
		}
		resp, err = smap.client.Post(smap.host, MIME_JSON, &buf)
		if err != nil {
			return errors.Wrap(err, "Could not post metadata to sMAP archiver")
		}
		if resp.StatusCode != 200 {
			return errors.Wrap(errors.New(resp.Status), "Could not save data to sMAP archiver")
		}
	}

	return nil
}

func (smap *sMAPSource) LoadData(c *cli.Context) error {
	var resp *http.Response
	var err error
	file, err := os.Open(c.String("datafile"))
	if err != nil {
		return errors.Wrap(err, "Could not open metadata file")
	}

	var alldata = make(map[string][]reading)

	var reader = csv.NewReader(file)
	reader.Read() // skip first line
	var linenum = 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "Could not read CSV data file")
		}
		linenum += 1
		if linenum%1000 == 0 {
			log.Debugf("Processed %d lines", linenum)
		}
		if len(line) != 3 {
			return fmt.Errorf("Line had more than 3 entries: %v", line)
		}
		uuid := line[0]
		time, err := time.Parse(time.RFC3339, line[1])
		if err != nil {
			return errors.Wrap(err, "Could not parse timestamp")
		}
		value, err := strconv.ParseFloat(line[2], 64)
		if err != nil {
			return errors.Wrap(err, "Could not parse value as float")
		}
		newReading := reading{Time: uint64(time.Unix() * 1e9), Value: value}

		if readings, found := alldata[uuid]; found {
			alldata[uuid] = append(readings, newReading)
		} else {
			alldata[uuid] = []reading{newReading}
		}

		if len(alldata[uuid]) >= 1000 {
			msg := smapMessage{UUID: uuid, Readings: alldata[uuid]}
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			if err := enc.Encode(map[string]interface{}{msg.Path: msg}); err != nil {
				return errors.Wrap(err, "Could not ingest data")
			}
			log.Debugf("Posting data to %v: %d", smap.host, buf.Len())
			resp, err = smap.client.Post(smap.host, MIME_JSON, &buf)
			if err != nil {
				return errors.Wrap(err, "Could not post data to sMAP archiver")
			}
			if resp.StatusCode != 200 {
				return errors.Wrap(errors.New(resp.Status), "Could not save data to sMAP archiver")
			}
			delete(alldata, uuid)
		}
	}

	for uuid, readings := range alldata { // last cleanup
		msg := smapMessage{UUID: uuid, Readings: readings}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		if err := enc.Encode(map[string]interface{}{msg.Path: msg}); err != nil {
			return errors.Wrap(err, "Could not ingest data")
		}
		resp, err = smap.client.Post(smap.host, MIME_JSON, &buf)
		if err != nil {
			return errors.Wrap(err, "Could not post data to sMAP archiver")
		}
		if resp.StatusCode != 200 {
			return errors.Wrap(errors.New(resp.Status), "Could not save data to sMAP archiver")
		}
	}
	return nil
}

type smapMessage struct {
	// Readings for this message
	Readings   []reading              `json:",omitempty"`
	UUID       string                 `json:"uuid"`
	Properties map[string]interface{} `json:",omitempty"`
	Metadata   map[string]interface{} `json:",omitempty"`
	Path       string                 `json:",omitempty"`
}

// Reading implementation for numerical data
type reading struct {
	// uint64 timestamp
	Time uint64
	// value associated with this timestamp
	Value float64
}

func (rdg *reading) MarshalJSON() ([]byte, error) {
	floatString := strconv.FormatFloat(rdg.Value, 'f', -1, 64)
	timeString := strconv.FormatUint(rdg.Time, 10)
	return json.Marshal([]json.Number{json.Number(timeString), json.Number(floatString)})
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
