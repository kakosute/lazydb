package lazydb

import (
	"errors"
	"lazydb/logfile"
	"math/rand"
	"sync"

	"github.com/bwmarrin/snowflake"
)

type (
	TxType   int
	TxStatus int
)

var (
	RTX        TxType   = 0
	RWTX       TxType   = 1
	pending    TxStatus = 0
	committing TxStatus = 1
)

var (
	ErrTxClosed             = errors.New("transaction is closed")
	ErrTxCommittingRollback = errors.New("transaction rollback while committing")
)

type pSet struct {
	e   *logfile.LogEntry
	sum []byte
	mem []byte
}

type Tx struct {
	id          uint64
	db          *LazyDB
	tType       TxType
	status      TxStatus
	pendingStr  []*logfile.LogEntry
	pendingList []*logfile.LogEntry
	pendingSet  []*pSet
	pendingHash []*logfile.LogEntry
	pendingZSet []*logfile.LogEntry
}

func generateTxID() (uint64, error) {
	nodeID := rand.Int63() % 1023
	node, err := snowflake.NewNode(nodeID)
	if err != nil {
		return 0, err
	}

	id := uint64(node.Generate().Int64())
	return id, err
}

func newTx(db *LazyDB, txType TxType) (*Tx, error) {
	txID, err := generateTxID()
	if err != nil {
		return nil, err
	}

	tx := &Tx{
		id:          txID,
		db:          db,
		tType:       txType,
		status:      pending,
		pendingStr:  []*logfile.LogEntry{},
		pendingList: []*logfile.LogEntry{},
		pendingHash: []*logfile.LogEntry{},
		pendingSet:  []*pSet{},
		pendingZSet: []*logfile.LogEntry{},
	}

	return tx, nil
}

func (tx *Tx) lock() {
	if tx.tType == RWTX {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

func (tx *Tx) unlock() {
	if tx.tType == RWTX {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

func (tx *Tx) IsClosed() bool {
	return tx.db == nil
}

func (db *LazyDB) Begin(txType TxType) (*Tx, error) {
	tx, err := newTx(db, txType)
	if err != nil {
		return nil, err
	}

	if db.IsClosed() {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	tx.lock()

	return tx, nil
}

func (tx *Tx) Rollback() error {
	if tx.IsClosed() {
		return ErrTxClosed
	}

	if tx.db.IsClosed() {
		return ErrDatabaseClosed
	}

	if tx.status == committing {
		return ErrTxCommittingRollback
	}

	tx.unlock()

	tx.db = nil
	tx.pendingStr = nil
	tx.pendingSet = nil
	tx.pendingList = nil
	tx.pendingZSet = nil
	tx.pendingHash = nil
	tx.status = pending

	return nil
}

func (tx *Tx) Commit() error {
	if tx.IsClosed() {
		return ErrTxClosed
	}

	if tx.db.IsClosed() {
		return ErrDatabaseClosed
	}

	if tx.status == committing {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(5)

	go func() {
		defer wg.Done()
		for _, e := range tx.pendingStr {
			e.TxStat = logfile.TxCommited
			valuePos, _ := tx.db.writeLogEntry(valueTypeString, e)
			tx.db.updateIndexTree(valueTypeString, tx.db.strIndex.idxTree, e, valuePos, true)
		}
	}()

	go func() {
		defer wg.Done()
		for _, e := range tx.pendingList {
			e.TxStat = logfile.TxCommited
			valuePos, _ := tx.db.writeLogEntry(valueTypeList, e)
			tx.db.updateIndexTree(valueTypeList, tx.db.strIndex.idxTree, e, valuePos, true)
		}
	}()

	go func() {
		defer wg.Done()
		for _, e := range tx.pendingHash {
			e.TxStat = logfile.TxCommited
			valuePos, _ := tx.db.writeLogEntry(valueTypeHash, e)
			tx.db.updateIndexTree(valueTypeHash, tx.db.strIndex.idxTree, e, valuePos, true)
		}
	}()

	go func() {
		defer wg.Done()
		for _, ps := range tx.pendingSet {
			ps.e.TxStat = logfile.TxCommited
			valuePos, _ := tx.db.writeLogEntry(valueTypeSet, ps.e)

			entry := &logfile.LogEntry{Key: ps.sum, Value: ps.mem}
			_, size := logfile.EncodeEntry(ps.e)
			valuePos.entrySize = size

			idxTree := tx.db.setIndex.trees[string(ps.e.Key)]
			tx.db.updateIndexTree(valueTypeSet, idxTree, entry, valuePos, false)

		}
	}()

	go func() {
		defer wg.Done()
		for _, e := range tx.pendingZSet {
			e.TxStat = logfile.TxCommited
			valuePos, _ := tx.db.writeLogEntry(valueTypeZSet, e)
			tx.db.updateIndexTree(valueTypeZSet, tx.db.strIndex.idxTree, e, valuePos, true)
		}
	}()

	wg.Wait()

	tx.unlock()

	tx.db = nil
	tx.pendingStr = nil
	tx.pendingSet = nil
	tx.pendingList = nil
	tx.pendingZSet = nil
	tx.pendingHash = nil
	tx.status = pending

	return nil
}
