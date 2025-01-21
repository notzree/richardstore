package main

// func TestLocalMetadataPersistor(t *testing.T) {
// 	original_data := &PersistedRaftFields{
// 		CurrentTerm: 4,
// 		VotedFor:    2,
// 	}
// 	p := NewFsSnapShotStore[PersistedRaftFields]("test/data.gob")
// 	if err := p.Persist(*original_data); err != nil {
// 		log.Printf("error persisting data: %s", err)
// 	}

// 	retrieved_data, err := p.GetMostRecent()
// 	if err != nil {
// 		log.Printf("error retrieving data %s", err)
// 	}
// 	assert.Equal(t, original_data, retrieved_data)
// }
