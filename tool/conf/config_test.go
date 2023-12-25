package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {

	err := LoadConfig("/Users/mac/source/easyredis/test.conf")

	assert.Equal(t, nil, err)

	assert.Equal(t, "127.0.0.1", GlobalConfig.Bind)
	assert.Equal(t, 3000, GlobalConfig.Port)
	assert.Equal(t, "/opt/data", GlobalConfig.Dir)
	assert.Equal(t, true, GlobalConfig.AppendOnly)
	assert.Equal(t, "", GlobalConfig.AppendFilename)
	assert.Equal(t, "", GlobalConfig.AppendFsync)
	assert.Equal(t, "20231224", GlobalConfig.RequirePass)
	assert.Equal(t, []string{"192.168.1.10", "192.168.1.10"}, GlobalConfig.Peers)

	t.Log(GlobalConfig.RunID)

}


