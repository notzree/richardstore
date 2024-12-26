package main

type DataManager struct {
	DataTransport
	Store
}

func (dm *DataManager) Has(hash string) bool {
	return dm.Store.Has(hash)
}

func (dm *DataManager) Listen() error {
	return dm.Start()
}
