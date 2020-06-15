/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/orderer/common/resolver"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver defines a sink for the ordered broadcast messages
type Receiver interface {
	// Ordered should be invoked sequentially as messages are ordered
	// Each batch in `messageBatches` will be wrapped into a block.
	// `pending` indicates if there are still messages pending in the receiver.
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut returns the current batch and starts a new one
	Cut() []*cb.Envelope
}

type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*cb.Envelope
	pendingBatchSizeBytes uint32

	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics
	readKeyMap            map[string][]int32 //maps readKey -> list of txid
	writeKeyMap           map[string][]int32 //maps writeKey to list of txid
	txDepGraph            map[int32][]int32  // txDepGraph[txID] == list of all transactions that this transaction conflicts with
	reorderList           []int32            //currently unused. Might be used in the future
	txIDCounter           int32
}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(channelID string, sharedConfigFetcher OrdererConfigFetcher, metrics *Metrics) Receiver {
	return &receiver{
		sharedConfigFetcher: sharedConfigFetcher,
		Metrics:             metrics,
		ChannelID:           channelID,
		readKeyMap:          make(map[string][]int32),
		writeKeyMap:         make(map[string][]int32),
		txDepGraph:          make(map[int32][]int32),
		reorderList:         make([]int32, 4),
		txIDCounter:         1,
	}
}

// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - impossible, as we have just received a message
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
// messageBatches length: 1, pending: false
//   - the message count reaches BatchSize.MaxMessageCount
// messageBatches length: 1, pending: true
//   - the current message will cause the pending batch size in bytes to exceed BatchSize.PreferredMaxBytes.
// messageBatches length: 2, pending: false
//   - the current message size in bytes exceeds BatchSize.PreferredMaxBytes, therefore isolated in its own batch.
// messageBatches length: 2, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.Cut()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")

	readSet, writeSet := decodeReadWriteSet(msg)
	if readSet == nil || writeSet == nil {
		logger.Debugf("readSet or writeSet failed to decode")
	} else {
		txID := r.txIDCounter
		//Check for WW conflicts
		for key := range writeSet {
			_, keyHasBeenWritten := r.writeKeyMap[key]
			if keyHasBeenWritten {
				//write-write conflict
				return
			}
		}

		//Check for WR conflicts
		for key := range readSet {
			conflictingTransactions, keyHasBeenWritten := r.writeKeyMap[key]
			if keyHasBeenWritten {
				//write-read conflict
				r.reorderList = append(r.reorderList, txID)
				r.txDepGraph[txID] = append(r.txDepGraph[txID], conflictingTransactions...)
			}
		}

		//Add read-set and write-set to dependency graph
		for key := range readSet {
			r.readKeyMap[key] = append(r.readKeyMap[key], txID)
		}
		for key := range writeSet {
			r.writeKeyMap[key] = append(r.writeKeyMap[key], txID)
		}
	}

	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	r.txIDCounter++

	return
}

// Cut returns the current batch and starts a new one
func (r *receiver) Cut() []*cb.Envelope {
	r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	r.PendingBatchStartTime = time.Time{}
	batch := r.pendingBatch

	enableReorder := true
	if enableReorder {
		//reorder transactions using tarjanSCC and JohnsonCE
		graph := make([][]int32, r.txIDCounter)
		invgraph := make([][]int32, r.txIDCounter)

		for txID, conflictingTransactions := range r.txDepGraph {
			for conflictingTxID := range conflictingTransactions {
				graph[txID] = append(graph[txID], int32(conflictingTxID))
				invgraph[conflictingTxID] = append(invgraph[conflictingTxID], txID)
			}
		}

		// scheduleSerializer := resolver.NewResolver(&graph, &invgraph)
		resolver.NewResolver(&graph, &invgraph)
		/*
			newSchedule, _ := scheduleSerializer.GetSchedule()

			serializedBatch := make([]*cb.Envelope, len(newSchedule))
			for i := 0; i < len(newSchedule); i++ {
				serializedBatch[i] = batch[newSchedule[(len(newSchedule)-1)-i]]
			}

			batch = serializedBatch
		*/
	}

	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0

	//clear values for next block
	r.readKeyMap = make(map[string][]int32)
	r.writeKeyMap = make(map[string][]int32)
	r.txDepGraph = make(map[int32][]int32)
	r.reorderList = make([]int32, 4)
	r.txIDCounter = 1

	return batch
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}

func decodeReadWriteSet(msg *cb.Envelope) (map[string]string, map[string]string) {
	var err error
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Infof("proto data error")
		return nil, nil
	}

	payload, err := utils.GetActionFromEnvelope(data)
	if err != nil {
		logger.Infof("Error with payload")
		return nil, nil
	}

	txRWSet := &rwsetutil.TxRwSet{}
	err = txRWSet.FromProtoBytes(payload.Results)
	if err != nil {
		logger.Infof("txrwset error")
		return nil, nil
	}

	readSet := make(map[string]string)
	writeSet := make(map[string]string)
	for _, ns := range txRWSet.NsRwSets[1:] {
		for _, write := range ns.KvRwSet.Writes {
			writeKey := write.GetKey()
			writeSet[writeKey] = string(write.GetValue())
		}
		for _, read := range ns.KvRwSet.Reads {
			key := read.GetKey()
			readSet[key] = fmt.Sprintf("%v", read.GetVersion())
		}
	}

	return readSet, writeSet
}
