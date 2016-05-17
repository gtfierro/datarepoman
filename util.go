package main

import (
	"github.com/codegangsta/cli"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

func setLogLevel(c *cli.Context) {
	level, err := logging.LogLevel(c.GlobalString("debuglevel"))
	if err != nil {
		log.Error(errors.Wrap(err, "Could not parse log level"))
		level = logging.INFO
	}
	log.Info("Setting log level to", level)
	logging.SetLevel(level, "datarepoman")
}

// given some download parameter, chunks them by UUID so they can
// be processed independently
func getUUIDChunks(params *downloadParams) chan downloadParams {
	chunks := make(chan downloadParams)

	go func(chunks chan downloadParams, params *downloadParams) {
		idx := 0
		chunk := downloadParams{
			start: params.start,
			end:   params.end,
		}
		for idx <= len(params.uuids) {
			chunk.uuids = params.uuids[idx : idx+params.uuidChunkSize]
			idx += params.uuidChunkSize
			log.Infof("Generating chunk %d/%d", (idx / params.uuidChunkSize), (len(params.uuids)/params.uuidChunkSize)+1)
			chunks <- chunk
		}
	}(chunks, params)

	return chunks
}
