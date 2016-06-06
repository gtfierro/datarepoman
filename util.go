package main

import (
	"github.com/codegangsta/cli"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"time"
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
		current := time.Now()
		chunk := downloadParams{
			start:  params.start,
			end:    params.end,
			legacy: params.legacy,
		}
		for idx <= len(params.uuids) {
			upperBound := idx + params.uuidChunkSize
			if len(params.uuids) == 1 {
				upperBound = 1
			} else if upperBound >= len(params.uuids) {
				upperBound = len(params.uuids) - 1
			}
			chunk.uuids = params.uuids[idx:upperBound]
			idx += params.uuidChunkSize
			log.Infof("Generating chunk %d/%d -- %s", (idx / params.uuidChunkSize), (len(params.uuids)/params.uuidChunkSize)+1, time.Since(current))
			current = time.Now()
			chunks <- chunk
		}
		close(chunks)
	}(chunks, params)

	return chunks
}
