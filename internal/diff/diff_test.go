package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexName(t *testing.T) {
	assert.Equal(t, "idx_a_b", getIndexName("a", "b"))
	assert.Equal(t, "idx_a_b", getIndexName("A", "b"))
	assert.Equal(t, "idx_a_b", getIndexName("A", "B"))
	assert.Equal(t, "idx_a_b", getIndexName("a", "B"))
}
