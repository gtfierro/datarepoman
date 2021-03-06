package main

import (
	"github.com/codegangsta/cli"
	"github.com/op/go-logging"
	"github.com/pkg/profile"
	"os"
)

var VERSION = "0.0.1"
var log = logging.MustGetLogger("datarepoman")
var format = "%{color}%{level} %{time:Jan 02 15:04:05} %{shortfile}%{color:reset} ▶ %{message}"

func init() {
	var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
	logBackendLeveled := logging.AddModuleLevel(logBackend)
	logging.SetBackend(logBackendLeveled)
	logging.SetFormatter(logging.MustStringFormatter(format))
}

func main() {
	app := cli.NewApp()
	app.Name = "datarepoman"
	app.Version = VERSION
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "sourcetype, s",
			Value: "smap",
			Usage: "Type of source to download smap, readingdb, btrdb, etc.",
		},
		cli.StringFlag{
			Name:  "address, url",
			Value: "http://localhost:8079/api/query",
			Usage: "Address to connect to for the provided sourcetype",
		},
		cli.StringFlag{
			Name:  "debuglevel, debug, level",
			Value: "debug",
			Usage: "How much output you get. In decreasing verbosity: debug, info, warn, error, critical",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "download",
			Aliases: []string{"dl"},
			Usage:   "Download data from some external source, e.g. sMAP, ReadingDB, BtrDB",
			Action:  Download,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "uuids",
					Value: "",
					Usage: "List of UUIDs to download. Can also use -a for all streams",
				},
				cli.BoolFlag{
					Name:  "all, a",
					Usage: "If specified, downloads *all* streams from the data source. Overrides -uuids",
				},
				cli.StringFlag{
					Name:  "where",
					Usage: "Metadata query to download UUIDs",
				},
				//TODO: document  the formats
				cli.StringFlag{
					Name:  "from,f",
					Value: "0", //Unix Time
					Usage: "Start date of data download. Currently in Unix epoch time",
				},
				cli.StringFlag{
					Name:  "to,t",
					Value: "0", //Unix Time
					Usage: "End date of data download. Currently in Unix epoch time",
				},
				cli.DurationFlag{
					Name:  "timeout,w",
					Value: 0, // no timeout
					Usage: "Timeout using Go's time.Duration syntax. 0 means no timeout",
				},
				cli.IntFlag{
					Name:  "uuidchunk,chunk",
					Value: 10,
					Usage: "How many UUIDs to fetch at a time",
				},
				cli.BoolFlag{
					Name:  "legacy",
					Usage: "True if we are talking to sMAP python archiver 2.0",
				},
			},
		},
		{
			Name:   "metadata",
			Usage:  "Download metadata",
			Action: Metadata,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "uuids",
					Value: "",
					Usage: "List of UUIDs to download. Can also use -a for all streams",
				},
				cli.BoolFlag{
					Name:  "all, a",
					Usage: "If specified, downloads *all* streams from the data source. Overrides -uuids",
				},
				cli.StringFlag{
					Name:  "where",
					Usage: "Metadata query to download UUIDs",
				},
			},
		},
		{
			Name:   "ingest",
			Usage:  "Load data in via sMAP",
			Action: Ingest,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "metadata,md",
					Value: "metadata.json",
					Usage: "JSON file to load metadata from",
				},
				cli.StringFlag{
					Name:  "datafile",
					Value: "out.csv",
					Usage: "CSV file of data: uuid,time,value",
				},
			},
		},
	}

	defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()

	app.Run(os.Args)
}
