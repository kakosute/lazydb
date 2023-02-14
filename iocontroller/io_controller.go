package iocontroller

type IOController interface {
	// Write a slice to log file at offset.
	// It returns the number of bytes written and any error encountered.
	Write(b []byte, offset int64) (int, error)

	// Read a slice from offset.
	// It returns the number of bytes read and any error encountered.
	Read(b []byte, offset int64) (int, error)

	// Sync commits the current contents of the file from memory to stable storage.
	Sync() error

	// Close closes the File.
	Close() error

	// Delete delete the file.
	Delete() error
}
