package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
}

// NewStandAloneStorage create a new StandAloneStorage
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	db := engine_util.CreateDB(dbPath, false)
	engines := engine_util.NewEngines(db, nil, kvPath, "")
	return &StandAloneStorage{engines: engines}
}

// Start starts the given StandAloneStorage instance
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

// Stop the given StandAloneStorage instance
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Kv.Close()
}

// Reader returns a reader to read the storage
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	reader := &BadgerReader{txn}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			wb := engine_util.WriteBatch{}
			wb.SetCF(put.Cf, put.Key, put.Value)
			return s.engines.WriteKV(&wb)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb := engine_util.WriteBatch{}
			wb.SetCF(delete.Cf, delete.Key, nil)
			return s.engines.WriteKV(&wb)
		}
	}
	return nil
}

// BadgerReader is a wrapper for badge txn
type BadgerReader struct {
	txn *badger.Txn
}

// GetCF find the value by cf and key
func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// IterCF iterate by cf
func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

// Close the Reader
func (b *BadgerReader) Close() {
	b.txn.Discard()
}
