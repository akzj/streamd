package engine

import (
	"context"
	"encoding/binary"
	"testing"
)

func BenchmarkAppendSingleSync(b *testing.B) {
	store, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	payload := make([]byte, 1024)
	requestID := make([]byte, 8)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(requestID, uint64(i))
		_, err = store.Append(context.Background(), AppendRequest{Namespace: "bench", Stream: "events", ExpectedSequence: uint64(i), RequestID: requestID, Producer: "benchmark", Records: []InputRecord{{Payload: payload}}})
		if err != nil {
			b.Fatal(err)
		}
	}
}
