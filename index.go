package lazydb

func (db *LazyDB) getValue(key []byte, typ valueType) ([]byte, error) {
	rawValue, ok := db.keyIndex.Get(string(key))
	if !ok {
		return nil, ErrKeyNotFound

	}
	val, _ := rawValue.(*Value)
	if val == nil {
		return nil, ErrKeyNotFound
	}

	// TODO: check expire time later

	logFile := db.getCurLogFile(typ)
	if logFile.Fid != val.fid {
		logFile = db.getArchivedLogFile(typ, val.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotExist
	}

	ent, _, err := logFile.ReadLogEntry(val.offset)
	if err != nil {
		return nil, err
	}
	// TODO: !get key deletion status

	return ent.Value, nil
}
