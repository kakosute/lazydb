package lazydb

import (
	"lazydb/util"
)

func (db *LazyDB) getValue(key []byte, typ valueType) ([]byte, error) {
	rawValue, ok := db.index.Get(util.ByteToString(key))
	if !ok {
		return nil, ErrKeyNotFound

	}
	val, _ := rawValue.(*Value)
	if val == nil {
		return nil, ErrKeyNotFound
	}

	// TODO: check expire time later

	ent, err := db.readLogEntry(typ, val.fid, val.offset)
	if err != nil {
		return nil, err
	}

	// TODO: !get key deletion status

	return ent.Value, nil
}
