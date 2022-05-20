package main

import (
	"context"
	"encoding/binary"
	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/base"
	"github.com/pion/rtp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"math"
	"sync/atomic"
	"time"
)

const dbUri = "mongodb://localhost:27017"
const maxTracks = 10
const waitTime = 5

//const streamUrl = "rtsp://rtsp.stream/pattern"

const streamUrl = "rtsp://localhost:8554/mystream"

func main() {
	var ctx context.Context
	var cancel context.CancelFunc
	const maxNTP = math.MaxInt64

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dbUri))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		panic(err)
	}

	log.Println("Successfully connected and pinged database.")
	collection := client.Database("testing").Collection("packets")

	var signals [maxTracks]uint32
	for index, _ := range signals {
		signals[index] = 0
	}

	u, err := base.ParseURL(streamUrl)
	if err != nil {
		panic(err)
	}

	c := gortsplib.Client{}

	err = c.Start(u.Scheme, u.Host)
	if err != nil {
		panic(err)
	}

	tracks, baseURL, _, err := c.Describe(u)
	if err != nil {
		err := c.Close()
		if err != nil {
			return
		}
	}

	c.OnPacketRTP = func(ctx *gortsplib.ClientOnPacketRTPCtx) {

		Swap := atomic.CompareAndSwapUint32(&signals[ctx.TrackID], 1, 0)
		if Swap {
			writePacket(ctx.TrackID, ctx.Packet, collection)
		}

	}

	err = c.SetupAndPlay(tracks, baseURL)
	if err != nil {
		panic(err)
	}
	// wait until a fatal error

	log.Println("Starting service...")

	//panic(c.Wait())

	// TODO: make a separate goroutine
	for {
		for index, _ := range signals {
			atomic.StoreUint32(&signals[index], 1)
		}
		time.Sleep(waitTime * time.Second)
	}

}

func writePacket(trackID int, pkt *rtp.Packet, collection *mongo.Collection) {
	ntpExtension := binary.LittleEndian.Uint64(pkt.GetExtension(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log.Println("TrackID: ", trackID, "\n", pkt, "NTP: ", ntpExtension)
	_, err := collection.InsertOne(ctx, bson.D{
		{"track", trackID},
		{"NTP", ntpExtension},
		{"timestamp", pkt.Timestamp},
		{"sequence_number", pkt.SequenceNumber},
		{"payload_type", pkt.PayloadType},
		{"payload_length", len(pkt.Payload)},
	})
	if err != nil {
		panic(err)
	}
}
