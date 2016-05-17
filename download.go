package main

import (
	"github.com/codegangsta/cli"
	"github.com/pkg/errors"
)

func download(c *cli.Context) {
	setLogLevel(c)
	var downloader source
	switch sourcetype := c.GlobalString("sourcetype"); sourcetype {
	case "smap":
		log.Debug("Using sMAP source type")
		downloader = SmapSourceFromParams(c)
	case "readingdb":
		log.Fatal("ReadingDB support not implemented")
	case "btrdb":
		log.Fatal("BtrDB support not implemented")
	default:
		log.Fatalf("Do not understand source type '%s'", sourcetype)
	}

	err := downloader.Connect()
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not connect"))
	}
	//downloader.AddDestination(&StdoutDestination{})
	downloader.AddDestination(CreateCSVDestination("out.csv"))
	if err := downloader.Download(c); err != nil {
		log.Fatal(errors.Wrap(err, "Could not download data"))
	}
}

func metadata(c *cli.Context) {
	setLogLevel(c)
	var downloader source
	switch sourcetype := c.GlobalString("sourcetype"); sourcetype {
	case "smap":
		log.Debug("Using sMAP source type")
		downloader = SmapSourceFromParams(c)
	default:
		log.Fatal("Need to use sMAP source type for metadata")
	}
	err := downloader.Connect()
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not connect"))
	}
	//downloader.AddDestination(CreateJSONDestination("metadata.json"))
	if err := downloader.GetMetadata(c); err != nil {
		log.Fatal(errors.Wrap(err, "Could not download metadata"))
	}
}

func ingest(c *cli.Context) {
	setLogLevel(c)
	var downloader source
	switch sourcetype := c.GlobalString("sourcetype"); sourcetype {
	case "smap":
		log.Debug("Using sMAP source type")
		downloader = SmapSourceFromParams(c)
	default:
		log.Fatal("Need to use sMAP source type for metadata")
	}
	err := downloader.Connect()
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not connect"))
	}
	if err := downloader.LoadMetadata(c); err != nil {
		log.Fatal(errors.Wrap(err, "Could not load metadata"))
	}
	if err := downloader.LoadData(c); err != nil {
		log.Fatal(errors.Wrap(err, "Could not load data"))
	}
}
