package lazydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTx(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	tx, err := db.Begin(RWTX)
	assert.NoError(t, err)

	tx.Set([]byte("1"), []byte("val1"))
	tx.SAdd([]byte("add2"), [][]byte{[]byte("v1")}...)
	tx.Set([]byte("3"), []byte("val3"))

	err = tx.Commit()
	assert.NoError(t, err)

	val, err := db.Get([]byte("1"))
	assert.NoError(t, err)
	assert.Equal(t, string(val), "val1")

	got := db.SIsMember([]byte("add2"), []byte("v1"))
	assert.Equal(t, got, true)

	val, err = db.Get([]byte("3"))
	assert.NoError(t, err)
	assert.Equal(t, string(val), "val3")

	// new transaction
	tx, err = db.Begin(RWTX)
	assert.NoError(t, err)

	tx.Set([]byte("4"), []byte("val4"))
	tx.Set([]byte("5"), []byte("val5"))
	err = tx.Rollback()
	assert.NoError(t, err)

	val, err = db.Get([]byte("4"))
	assert.Error(t, err)

	val, err = db.Get([]byte("5"))
	assert.Error(t, err)

}
